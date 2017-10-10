package ru.opentech.spring.jdbc.core;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.lang.Nullable;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleObjectExtractor implements ResultSetExtractor<Iterable<Map<String, Object>>> {

    @Nullable
    @Override
    public Iterable<Map<String, Object>> extractData( ResultSet rs ) throws SQLException, DataAccessException {
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        List<Map<String, Object>> list = new ArrayList<>();
        while( rs.next() ) {
            Map<String, Object> row = new HashMap<>();
            for( int i = 1; i <= resultSetMetaData.getColumnCount(); ++i ) {
                row.put( resultSetMetaData.getColumnLabel( i ), rs.getObject( i ) );
            }
            list.add( row );
        }
        return list;
    }

}
