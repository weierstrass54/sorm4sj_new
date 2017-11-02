package ru.opentech.spring.jdbc.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.lang.Nullable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityNotFoundException;
import javax.persistence.MappedSuperclass;
import javax.validation.constraints.NotNull;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.time.LocalTime;
import java.util.*;
import java.util.Date;

/**
 * Обработчик запросов SQL с маппингом данных через аннотации JPA
 * @param <T> тип объекта, к которому нужно привести строку результата SQL запроса
 */
public class JpaAnnotationProcessor<T> implements ResultSetExtractor<Iterable<T>> {

    private static final Logger log = LoggerFactory.getLogger( JpaAnnotationProcessor.class );

    // конструктор объекта, к которому маппится строка результата SQL запроса
    private final Constructor<T> constructor;

    // список полей класса, к которым нужно привязать значения результатов запроса
    private final List<Field> columnFields;
    // список методов класса, которые нужно вызвать со значением результата запроса в виде входного параметра
    private final List<Method> columnMethods;

    // карта имен колонок и их индексов
    private Map<String, Integer> sqlColumnsMap;

    public JpaAnnotationProcessor( Class<T> clazz ) throws NoSuchMethodException {
        constructor = clazz.getDeclaredConstructor();
        this.columnFields = new ArrayList<>();
        this.columnMethods = new ArrayList<>();
        this.sqlColumnsMap = new HashMap<>();
        gatherMappedColumns( clazz );
    }

    @Nullable
    @Override
    public Iterable<T> extractData( ResultSet rs ) throws SQLException, DataAccessException {
        List<T> result = new ArrayList<>();
        try {
            buildSqlColumnsMap( rs.getMetaData() );
            while( rs.next() ) {
                T instance = constructor.newInstance();
                processFields( instance, rs );
                processMethods( instance, rs );
                result.add( instance );
            }
        }
        catch( Throwable t ) {
            log.error( "Ошибка получения данных из СУБД: {}", t.getMessage(), t );
            throw new SQLException( t );
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder( constructor.getDeclaringClass().getName() )
            .append( ": [" + "\n\tfields: [" );
        columnFields.forEach( f -> sb.append( "\n\t\t" + f.getName() ) );
        sb.append( "\n\t],\n\tmethods: [" );
        columnMethods.forEach( m -> sb.append( "\n\t\t" + m.getName() ) );
        sb.append( "\n\t]\n]" );
        return sb.toString();
    }

    /**
     * Обработка полей результирующего объекта
     * @param instance экземпляр результирующего объекта
     * @param rs даныне из СУБД
     * @throws IllegalAccessException в случае ошибки присвоения значения полю объекта
     * @throws SQLException в случае иных ошибок
     */
    private void processFields( T instance, ResultSet rs ) throws IllegalAccessException, SQLException {
        for( Field field : this.columnFields ) {
            log.debug( "Маппинг для поля {}", field.getName() );
            Integer index = sqlColumnsMap.get( field.getAnnotation( Column.class ).name() );
            // для реализации Deferred полей
            if( index == null ) {
                log.debug( "Поле {} не найдено в результате запроса СУБД. Пропускаю.." );
                continue;
            }
            Object value = convertSqlValue( index, rs );
            if( value == null && field.getAnnotation( NotNull.class ) != null ) {
                throw new SQLException( "Запрос вернул null значение для NotNull поля " + field.getName() );
            }
            log.debug( "Для поля {} класса {} получено значение {}", field.getName(), instance.getClass().getName(), value );
            field.setAccessible( true );
            field.set( instance, value );
        }
    }

    /**
     * Обработка методов результирующего объекта
     * @param instance экземпляр результирующего объекта
     * @param rs даныне из СУБД
     * @throws IllegalAccessException в случае, если указанный метод недоступен для вызова
     * @throws InvocationTargetException в случае, если указанного метода не существует
     * @throws SQLException в иных случаях
     */
    private void processMethods( T instance, ResultSet rs ) throws IllegalAccessException, InvocationTargetException, SQLException {
        for( Method method : this.columnMethods ) {
            log.debug( "Маппинг для метода {}( {} )", method.getName(), method.getParameters() );
            Integer index = sqlColumnsMap.get( method.getAnnotation( Column.class ).name() );
            if( index == null ) {
                log.debug( "Поле {} не найдено в результате запроса СУБД. Пропускаю.." );
                continue;
            }
            Object value = convertSqlValue( index, rs );
            if( value == null && method.getParameters()[0].getAnnotation( NotNull.class ) != null ) {
                throw new SQLException( "Запрос вернул null значение для NotNull входного параметра метода " + method.getName() );
            }
            log.debug( "Для метода {} класса {} получено значение {}", method.getName(), instance.getClass().getName(), value );
            method.setAccessible( true );
            method.invoke( instance, value );
        }
    }

    /**
     * Получение и хранение списка колонок и методов, к которым нужно привязать данные из результатов запроса СУБД
     * @param clazz класс, к экземплярам которого нужно привязывать данные
     */
    private void gatherMappedColumns( Class<T> clazz ) {
        if( clazz.getAnnotation( Entity.class ) == null && clazz.getAnnotation( MappedSuperclass.class ) == null ) {
            throw new EntityNotFoundException( "Результирующий класс " + clazz.getName() + " не помечен аннотацией " + Entity.class.getName() + " или " + MappedSuperclass.class.getName() );
        }
        for( Class clz = clazz; clz != Object.class; clz = clz.getSuperclass() ) {
            if( clz != clazz && clz.getAnnotation( MappedSuperclass.class ) == null ) {
                throw new EntityNotFoundException(
                    "Родитель " + clz.getName() + " результирующего класса " + clazz.getName() + " не помечен аннотацией " + MappedSuperclass.class.getName()
                );
            }
            for( Field field : clz.getDeclaredFields() ) {
                if( field.getAnnotation( Column.class ) != null ) {
                    this.columnFields.add( field );
                }
            }
            for( Method method : clz.getDeclaredMethods() ) {
                if( method.getAnnotation( Column.class ) != null ) {
                    if( method.getParameterCount() != 1 ) {
                        throw new EntityNotFoundException( "Метод " + method.getName() + " класса " + clz.getName() + " должен принимать на вход один параметр." );
                    }
                    this.columnMethods.add( method );
                }
            }
        }
        log.debug( "{}: {}", this.getClass(), this.toString() );
    }

    /**
     * Построение карты колонок результата запроса
     * @param resultSetMetaData мета-данные результата запроса
     * @throws SQLException в случае ошибки получения мета-данных
     */
    private void buildSqlColumnsMap( ResultSetMetaData resultSetMetaData ) throws SQLException {
        sqlColumnsMap.clear();
        for( int i = 1; i < resultSetMetaData.getColumnCount() + 1; ++i ) {
            sqlColumnsMap.put( resultSetMetaData.getColumnLabel( i ), i );
        }
    }

    /**
     * Конвертация SQL-значения в значение Java
     * @param index номер колонки
     * @param rs данные из СУБД
     * @return сконвертированное значение
     * @throws SQLException в случае любой ошибки конвертирования
     */
    private Object convertSqlValue( int index, ResultSet rs ) throws SQLException {
        switch( rs.getMetaData().getColumnType( index ) ) {
            case Types.BOOLEAN:
                log.debug( "Конвертирую значение результата запроса в Boolean колонки {}", rs.getMetaData().getColumnLabel( index ) );
                return rs.getBoolean( index );
            case Types.SMALLINT:
                log.debug( "Конвертирую значение результата запроса в Short колонки {}", rs.getMetaData().getColumnLabel( index ) );
                Short sh = rs.getShort( index );
                return !rs.wasNull() ? sh : null;
            case Types.INTEGER:
                log.debug( "Конвертирую значение результата запроса в Integer колонки {}", rs.getMetaData().getColumnLabel( index ) );
                Integer integer = rs.getInt( index );
                return !rs.wasNull() ? integer : null;
            case Types.BIGINT:
                log.debug( "Конвертирую значение результата запроса в Long колонки {}", rs.getMetaData().getColumnLabel( index ) );
                Long l = rs.getLong( index );
                return !rs.wasNull() ? l : null;
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                log.debug( "Конвертирую значение результата запроса в String колонки {}", rs.getMetaData().getColumnLabel( index ) );
                String string = rs.getString( index );
                return !rs.wasNull() ? string : null;
            case Types.FLOAT:
            case Types.REAL:
                log.debug( "Конвертирую значение результата запроса в Float колонки {}", rs.getMetaData().getColumnLabel( index ) );
                Float f = rs.getFloat( index );
                return !rs.wasNull() ? f : null;
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.DOUBLE:
                log.debug( "Конвертирую значение результата запроса в Double колонки {}", rs.getMetaData().getColumnLabel( index ) );
                Double d = rs.getDouble( index );
                return !rs.wasNull() ? d : null;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                log.debug( "Конвертирую значение результата запроса в LocalTime колонки {}", rs.getMetaData().getColumnLabel( index ) );
                Time time = rs.getTime( index );
                return !rs.wasNull() ? time.toLocalTime() : null;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                log.debug( "Конвертирую значение результата запроса в Date колонки {}", rs.getMetaData().getColumnLabel( index ) );
                Date date = rs.getTimestamp( index );
                return !rs.wasNull() ? date : null;
            default:
                throw new SQLException( "Не удалось сконвертировать колонку результата запроса" );
        }
    }

}
