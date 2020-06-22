package com.learning.utils;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.ResultSet;

public interface CustomRowMapper<T> extends JdbcIO.RowMapper<T> {

    public static class readTable implements JdbcIO.RowMapper<KV<String,String>>, Serializable {
        private static final long serialVersionUID = 1L;
        private static final Logger logger = LoggerFactory.getLogger(readTable.class);
        @Override
        public KV<String, String> mapRow(ResultSet resultSet) throws Exception {
            int colCount = resultSet.getMetaData().getColumnCount();

            StringBuilder result = new StringBuilder();
            for(int i=1;i<=colCount;i++) {
                String type = resultSet.getMetaData().getColumnTypeName(i);
                if(type.equalsIgnoreCase("varchar") || type.equalsIgnoreCase("char"))
                    result.append("\""+resultSet.getString(i)+"\",");
                if(type.equalsIgnoreCase("bit"))
                    result.append(resultSet.getString(i)+",");
                if(type.equalsIgnoreCase("tinyint"))
                    result.append(resultSet.getString(i)+",");
                if(type.equalsIgnoreCase("int"))
                    result.append(resultSet.getString(i)+",");
                if(type.equalsIgnoreCase("date"))
                    result.append("\""+resultSet.getString(i)+"\",");
                if(type.equalsIgnoreCase("datetime") || type.equalsIgnoreCase("time")){
                    String temp = resultSet.getString(i);
                    result.append("\""+resultSet.getString(i)+"\",");
                }

                if(type.equalsIgnoreCase("decimal")) {
                    if(resultSet.getBigDecimal(i).toString().equals("0.00"))
                        result.append(".00,");
                    else
                        result.append(resultSet.getBigDecimal(i)+",");
                }
            }
            result.setLength(result.length()-1);
            return (KV.of(resultSet.getMetaData().getTableName(1),result.toString()));
        }
    }


}

