package ru.opentech.spring.repository;

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.opentech.spring.jdbc.core.JpaAnnotationProcessor;

import java.util.List;

public abstract class ORMRepository<E> extends Repository {

    protected ORMRepository( NamedParameterJdbcTemplate jdbcTemplate ) {
        super( jdbcTemplate );
    }

    protected ORMRepository( NamedParameterJdbcTemplate jdbcTemplate, boolean attachCallerSrc ) {
        super( jdbcTemplate, attachCallerSrc );
    }

    /**
     * Выполенение запроса с безымянными параметрами к СУБД для получения списка кортежей с маппингом результата через JPA
     * @param clazz класс для маппинга
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список объектов, с привязанными значениями из результатов SQL запроса
     */
    protected <T extends E> List<T> loadList( Class<T> clazz, String query, Object... params ) {
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
    protected <T extends E> List<T> loadList( Class<T> clazz, String query, MapSqlParameterSource params ) {
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
    protected <T extends E> T loadObject( Class<T> clazz, String query, Object... params ) {
        return head( loadList( clazz, query, params ) );
    }

    /**
     * Выполенение запроса с именованными параметрами к СУБД для получения первого кортежа с маппингом результата через JPA
     * @param clazz класс для маппинга
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список объектов, с привязанными значениями из результатов SQL запроса
     */
    protected <T extends E> T loadObject( Class<T> clazz, String query, MapSqlParameterSource params ) {
        return head( loadList( clazz, query, params ) );
    }

    private <T extends E> ResultSetExtractor<List<T>> getResultSetExtractor( Class<T> clazz ) throws NoSuchMethodException {
        return new JpaAnnotationProcessor<>( clazz );
    }

}
