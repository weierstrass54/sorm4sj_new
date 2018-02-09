package ru.opentech.spring.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.*;
import java.util.stream.StreamSupport;

public abstract class Repository {

    protected final NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    protected Repository( NamedParameterJdbcTemplate jdbcTemplate ) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Выполнение запроса с безымянными параметрами к СУБД с извлечением из результата скаляра
     * @param clazz класс скаляра (Integer.class, String.class etc.)
     * @param query SQL-запрос
     * @param params безымянные параметры запроса
     * @param <T> тип скаляра
     * @return результат запроса в виде скаляра
     */
    protected <T> T loadScalar( Class<T> clazz, String query, Object... params ) {
        return jdbcTemplate.getJdbcOperations().queryForObject( query, clazz, prepare( params ) );
    }

    /**
     * Выполнение запроса c именованными параметрами к СУБД с извлечением из результата скаляра
     * @param clazz класс скаляра (Integer.class, String.class etc.)
     * @param query SQL-запрос
     * @param params именованные параметры запроса
     * @param <T> тип скаляра
     * @return результат запроса в виде скаляра
     */
    protected <T> T loadScalar( Class<T> clazz, String query, MapSqlParameterSource params ) {
        return jdbcTemplate.queryForObject( query, prepare( params ), clazz );
    }

    /**
     * Выполнение запроса с безымянными параметрами к СУБД с извлечением из результата одной строки в виде Map
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return Map, ключом которого является название колонки результата запроса, а значением - содержимое этой колонки
     */
    protected Map<String, Object> loadObject( String query, Object... params ) {
        return head( loadObjects( query, params ) );
    }

    /**
     * Выполнение запроса с именованными параметрами к СУБД с извлечением из результата одной строки в виде Map
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return Map, ключом которого является название колонки результата запроса, а значением - содержимое этой колонки
     */
    protected Map<String, Object> loadObject( String query, MapSqlParameterSource params ) {
        return head( loadObjects( query, params ) );
    }

    /**
     * Выполнение запроса с безымянными параметрами к СУБД с извлечением из результата одной колонки
     * @param clazz класс объектов колонки
     * @param query SQL-запрос
     * @param params параметры запроса
     * @param <T> тип данных колонки
     * @return колонка субд в виде списка объектов класса колонки
     */
    protected <T> List<T> loadColumn( Class<T> clazz, String query, Object... params ) {
        return Collections.unmodifiableList( jdbcTemplate.getJdbcOperations().queryForList( query, clazz, prepare( params ) ) );
    }

    /**
     * Выполнение запроса с именованными параметрами к СУБД с извлечением из результата одной колонки
     * @param clazz класс объектов колонки
     * @param query SQL-запрос
     * @param params параметры запроса
     * @param <T> тип данных колонки
     * @return колонка субд в виде списка объектов класса колонки
     */
    protected <T> List<T> loadColumn( Class<T> clazz, String query, MapSqlParameterSource params ) {
        return Collections.unmodifiableList( jdbcTemplate.queryForList( query, prepare( params ), clazz ) );
    }

    /**
     * Выполнение запроса с безымянными параметрами к СУБД с извлечением из результата всех строк в виде списка Map
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список Map, каждый из которых в ключах содержит название колонки результата запроса, а значение - содержимое этой колонки в текущей строке
     */
    protected List<Map<String, Object>> loadObjects( String query, Object... params ) {
        return Collections.unmodifiableList( jdbcTemplate.getJdbcOperations().queryForList( query, params ) );
    }

    /**
     * Выполнение запроса с именованными параметрами к СУБД с извлечением из результата всех строк в виде списка Map
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список Map, каждый из которых в ключах содержит название колонки результата запроса, а значение - содержимое этой колонки в текущей строке
     */
    protected List<Map<String, Object>> loadObjects( String query, MapSqlParameterSource params ) {
        return Collections.unmodifiableList( jdbcTemplate.queryForList( query, params ) );
    }

    /**
     * Выполенение запроса с безымянными параметрами к СУБД с извлечением из результат всех строк с преобразованием по заданному правилу
     * @param extractor правило преобразования результата запроса
     * @param query SQL-запрос
     * @param params параметры запроса
     * @param <T> тип данных, к которому нужно преобразовать строку результата запроса
     * @return список преобразованных данных
     */
    protected <T> List<T> loadList( ResultSetExtractor<List<T>> extractor, String query, Object... params ) {
        List<T> list = jdbcTemplate.getJdbcOperations().query( query, prepare( params ), extractor );
        return list != null ? Collections.unmodifiableList( list ) : new ArrayList<>();
    }

    /**
     * Выполенение запроса с именованными параметрами к СУБД с извлечением из результата всех строк с преобразованием по заданному правилу
     * @param extractor правило преобразования результата запроса
     * @param query SQL-запрос
     * @param params именованные параметры запроса
     * @param <T> тип данных, к которому нужно преобразовать строку результата запроса
     * @return список преобразованных данных
     */
    protected <T> List<T> loadList( ResultSetExtractor<List<T>> extractor, String query, MapSqlParameterSource params ) {
        List<T> list = jdbcTemplate.query( query, prepare( params ), extractor );
        return list != null ? Collections.unmodifiableList( list ) : new ArrayList<>();
    }

    /**
     * Выполнение запроса с безымянными параметрами к СУБД с возвращением кол-ва измененных строк
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return кол-во измененных строк
     */
    protected int execute( String query, Object... params ) {
        return jdbcTemplate.getJdbcOperations().update( query, prepare( params ) );
    }

    /**
     * Выполнение запроса с именованными параметрами к СУБД с возвращением кол-ва измененных строк
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return кол-во измененных строк
     */
    protected int execute( String query, MapSqlParameterSource params ) {
        return jdbcTemplate.update( query, prepare( params ) );
    }

    /**
     * Подготовка параметров запроса к выполнению.
     * Преобразовывает все перечисления в массив для передачи из в СУБД без преобразования в строку.
     * @param params список параметров
     * @return подготовленный список параметров
     */
    private Object[] prepare( Object... params ) {
        for( int i = 0; i < params.length; ++i ) {
            if( params[i] instanceof Iterable ) {
                params[i] = toArray( (Iterable)params[i] );
            }
        }
        return params;
    }

    /**
     * Подготовка именованных параметров к выполнению
     * @param params список параметров
     * @return подготовленный список параметров
     * @see #prepare(Object...)
     */
    private MapSqlParameterSource prepare( MapSqlParameterSource params ) {
        MapSqlParameterSource preparedParams = new MapSqlParameterSource();
        params.getValues().forEach(
            ( key, value ) -> {
                Object preparedValue = value instanceof Iterable ? toArray( (Iterable)value ) : value;
                preparedParams.addValue( key, preparedValue );
            }
        );
        return preparedParams;
    }

    /**
     * Получение первого элемента коллекции
     * @param iterable исходная коллекция
     * @param <T> тип данных коллекции
     * @return первый элемент результата запроса
     * @throws EmptyResultDataAccessException в случае, если коллекция пуста
     */
    protected static <T> T head( Iterable<T> iterable ) {
        Iterator<T> it = iterable.iterator();
        if( !it.hasNext() ) {
            throw new EmptyResultDataAccessException( 1 );
        }
        return it.next();
    }

    /**
     * Преобразование в массив объектов. Используется для преобразования коллекций в параметрах в массивы
     * @param iterable исходная коллекция
     * @return массив объектов
     */
    private static Object[] toArray( Iterable<?> iterable ) {
        return StreamSupport.stream( iterable.spliterator(), false ).toArray();
    }

}
