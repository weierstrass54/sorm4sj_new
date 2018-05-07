package ru.opentech.spring.repository;

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.opentech.spring.jdbc.core.JpaAnnotationProcessor;

import java.util.*;
import java.util.stream.StreamSupport;

public abstract class Repository {

    private final boolean attachCallerSrc;
    protected final NamedParameterJdbcTemplate jdbcTemplate;

    protected Repository( NamedParameterJdbcTemplate jdbcTemplate ) {
        this( jdbcTemplate, false );
    }

    protected Repository( NamedParameterJdbcTemplate jdbcTemplate, boolean attachCallerSrc ) {
        this.jdbcTemplate = jdbcTemplate;
        this.attachCallerSrc = attachCallerSrc;
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
        return jdbcTemplate.getJdbcOperations().queryForObject( prepare( query ), clazz, prepare( params ) );
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
        return jdbcTemplate.queryForObject( prepare( query ), prepare( params ), clazz );
    }

    /**
     * Выполнение запроса с безымянными параметрами к СУБД с извлечением из результата одной строки в виде Map
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return Map, ключом которого является название колонки результата запроса, а значением - содержимое этой колонки
     */
    protected Map<String, Object> loadObject( String query, Object... params ) {
        return head( loadObjects( prepare( query ), prepare( params ) ) );
    }

    /**
     * Выполнение запроса с именованными параметрами к СУБД с извлечением из результата одной строки в виде Map
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return Map, ключом которого является название колонки результата запроса, а значением - содержимое этой колонки
     */
    protected Map<String, Object> loadObject( String query, MapSqlParameterSource params ) {
        return head( loadObjects( prepare( query ), prepare( params ) ) );
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
        return Collections.unmodifiableList( jdbcTemplate.getJdbcOperations().queryForList( prepare( query ), clazz, prepare( params ) ) );
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
        return Collections.unmodifiableList( jdbcTemplate.queryForList( prepare( query ), prepare( params ), clazz ) );
    }

    /**
     * Выполнение запроса с безымянными параметрами к СУБД с извлечением из результата всех строк в виде списка Map
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список Map, каждый из которых в ключах содержит название колонки результата запроса, а значение - содержимое этой колонки в текущей строке
     */
    protected List<Map<String, Object>> loadObjects( String query, Object... params ) {
        return Collections.unmodifiableList( jdbcTemplate.getJdbcOperations().queryForList( prepare( query ), prepare( params ) ) );
    }

    /**
     * Выполнение запроса с именованными параметрами к СУБД с извлечением из результата всех строк в виде списка Map
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список Map, каждый из которых в ключах содержит название колонки результата запроса, а значение - содержимое этой колонки в текущей строке
     */
    protected List<Map<String, Object>> loadObjects( String query, MapSqlParameterSource params ) {
        return Collections.unmodifiableList( jdbcTemplate.queryForList( prepare( query ), prepare( params ) ) );
    }

    /**
     * Выполенение запроса с безымянными параметрами к СУБД с извлечением из результат всех строк с преобразованием по заданному правилу
     * @param clazz класс для маппинга
     * @param query SQL-запрос
     * @param params параметры запроса
     * @param <T> тип данных, к которому нужно преобразовать строку результата запроса
     * @return список преобразованных данных
     */
    protected <T> List<T> loadListOf( Class<T> clazz, String query, Object... params ) {
        try {
            List<T> list = jdbcTemplate.getJdbcOperations().query( prepare( query ), prepare( params ), getResultSetExtractor( clazz ) );
            return list != null ? Collections.unmodifiableList( list ) : new ArrayList<>();
        }
        catch( NoSuchMethodException e ) {
            throw new DataRetrievalFailureException( "Не удалось создать объект " + clazz.getName() + " для маппинга результата запроса СУБД." );
        }
    }

    /**
     * Выполенение запроса с именованными параметрами к СУБД с извлечением из результата всех строк с преобразованием по заданному правилу
     * @param clazz класс для маппинга
     * @param query SQL-запрос
     * @param params именованные параметры запроса
     * @param <T> тип данных, к которому нужно преобразовать строку результата запроса
     * @return список преобразованных данных
     */
    protected <T> List<T> loadListOf( Class<T> clazz, String query, MapSqlParameterSource params ) {
        try {
            List<T> list = jdbcTemplate.query( prepare( query ), prepare( params ), getResultSetExtractor( clazz ) );
            return list != null ? Collections.unmodifiableList( list ) : new ArrayList<>();
        }
        catch( NoSuchMethodException e ) {
            throw new DataRetrievalFailureException( "Не удалось создать объект " + clazz.getName() + " для маппинга результата запроса СУБД." );
        }
    }

    /**
     * Выполенение запроса с безымянными параметрами к СУБД для получения первого кортежа с маппингом результата через JPA
     * @param clazz класс для маппинга
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список объектов, с привязанными значениями из результатов SQL запроса
     */
    protected <T> T loadObjectOf( Class<T> clazz, String query, Object... params ) {
        return head( loadListOf( clazz, query, params ) );
    }

    /**
     * Выполенение запроса с именованными параметрами к СУБД для получения первого кортежа с маппингом результата через JPA
     * @param clazz класс для маппинга
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список объектов, с привязанными значениями из результатов SQL запроса
     */
    protected <T> T loadObjectOf( Class<T> clazz, String query, MapSqlParameterSource params ) {
        return head( loadListOf( clazz, query, params ) );
    }

    /**
     * Выполнение запроса с безымянными параметрами к СУБД с возвращением кол-ва измененных строк
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return кол-во измененных строк
     */
    protected int execute( String query, Object... params ) {
        return jdbcTemplate.getJdbcOperations().update( prepare( query ), prepare( params ) );
    }

    /**
     * Выполнение запроса с именованными параметрами к СУБД с возвращением кол-ва измененных строк
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return кол-во измененных строк
     */
    protected int execute( String query, MapSqlParameterSource params ) {
        return jdbcTemplate.update( prepare( query ), prepare( params ) );
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
     * Подготовить запрос к выполнению.
     * Использует настройку sorm4sj.attach-caller-src для того, что к началу запроса добавлять имя класса, метода
     * и номер строки, откуда был сделан вызов SQL-запроса.
     * @param query SQL-запрос
     * @return запрос с дополнительной информацией
     */
    private String prepare( final String query ) {
        if( attachCallerSrc ) {
            // todo: add application name
            String callerSrc = "";
            StackTraceElement source = Arrays.stream( new Throwable().getStackTrace() )
                .filter( trace -> trace.getClassName().equals( this.getClass().getName() ) )
                .findFirst().orElse( null );
            if( source != null ) {
                callerSrc = "/* " + source.getFileName() + ':' + source.getLineNumber() + " /*\n";
            }
            return callerSrc + query;
        }
        return query;
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

    private <T> ResultSetExtractor<List<T>> getResultSetExtractor( Class<T> clazz ) throws NoSuchMethodException {
        return new JpaAnnotationProcessor<>( clazz );
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
