package ru.opentech.spring.repository;

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.opentech.spring.jdbc.core.JpaAnnotationProcessor;

abstract public class ORMRepository<T> extends Repository {

    protected ORMRepository( NamedParameterJdbcTemplate jdbcTemplate ) {
        super( jdbcTemplate );
    }

    /**
     * Выполенение запроса с безымянными параметрами к СУБД для получения списка кортежей с маппингом результата через JPA
     * @param clazz класс для маппинга
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список объектов, с привязанными значениями из результатов SQL запроса
     */
    protected Iterable<T> loadList( Class<T> clazz, String query, Object... params ) {
        try {
            return super.loadList( getResultSetExtractor( clazz ), query, params );
        }
        catch( NoSuchMethodException e ) {
            throw new DataRetrievalFailureException( "Не удалось создать объект " + clazz.getName() + " для маппинга результата запроса СУБД." );
        }
    }

    /**
     * Выполенение запроса с именованными параметрами к СУБД для получения списка кортежей с маппингом результата через JPA
     * @param clazz класс для маппинга
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список объектов, с привязанными значениями из результатов SQL запроса
     */
    protected Iterable<T> loadList( Class<T> clazz, String query, MapSqlParameterSource params ) {
        try {
            return super.loadList( getResultSetExtractor( clazz ), query, params );
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
    protected T loadObject( Class<T> clazz, String query, Object... params ) {
        return head( loadList( clazz, query, params ) );
    }

    /**
     * Выполенение запроса с именованными параметрами к СУБД для получения первого кортежа с маппингом результата через JPA
     * @param clazz класс для маппинга
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список объектов, с привязанными значениями из результатов SQL запроса
     */
    protected T loadObject( Class<T> clazz, String query, MapSqlParameterSource params ) {
        return head( loadList( clazz, query, params ) );
    }

    private ResultSetExtractor<Iterable<T>> getResultSetExtractor( Class<T> clazz ) throws NoSuchMethodException {
        return new JpaAnnotationProcessor<>( clazz );
    }

}
