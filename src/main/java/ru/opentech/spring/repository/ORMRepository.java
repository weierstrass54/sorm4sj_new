package ru.opentech.spring.repository;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

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
        return super.loadListOf( clazz, query, params );
    }

    /**
     * Выполенение запроса с именованными параметрами к СУБД для получения списка кортежей с маппингом результата через JPA
     * @param clazz класс для маппинга
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список объектов, с привязанными значениями из результатов SQL запроса
     */
    protected <T extends E> List<T> loadList( Class<T> clazz, String query, MapSqlParameterSource params ) {
        return super.loadListOf( clazz, query, params );
    }

    /**
     * Выполенение запроса с безымянными параметрами к СУБД для получения первого кортежа с маппингом результата через JPA
     * @param clazz класс для маппинга, который наследует базовый класс ORMRepository
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список объектов, с привязанными значениями из результатов SQL запроса
     */
    protected <T extends E> T loadObject( Class<T> clazz, String query, Object... params ) {
        return head( loadList( clazz, query, params ) );
    }

    /**
     * Выполенение запроса с именованными параметрами к СУБД для получения первого кортежа с маппингом результата через JPA
     * @param clazz класс для маппинга, который наследует базовый класс ORMRepository
     * @param query SQL-запрос
     * @param params параметры запроса
     * @return список объектов, с привязанными значениями из результатов SQL запроса
     */
    protected <T extends E> T loadObject( Class<T> clazz, String query, MapSqlParameterSource params ) {
        return head( loadList( clazz, query, params ) );
    }

}
