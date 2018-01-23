package com.sink;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PostGreSqlUpdateAction {

    private final String functionalIdFieldName;
    private final int keyIndex;
    private final String[] fieldNames;
    private final String sql;
    private PreparedStatement preparedStatement;

    public PostGreSqlUpdateAction(String tableName, StructType schema, String functionalIdFieldName) {
        this.functionalIdFieldName = functionalIdFieldName;
        this.keyIndex = schema.fieldIndex(functionalIdFieldName);
        this.fieldNames = schema.fieldNames();
        this.sql = buildSqlUpdateQuery(tableName, functionalIdFieldName, fieldNames);
    }


    public void prepare(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        this.preparedStatement = connection.prepareStatement(sql);
    }

    public void process(Row row) {
        feedUpdateStatement(row);
    }

    public void execute(Connection connection) throws SQLException {
        preparedStatement.addBatch();
        this.preparedStatement.executeBatch();
        connection.commit();
    }

    public void close() throws SQLException {
        this.preparedStatement.close();
    }

    private String buildSqlUpdateQuery(String tableName, String keyFieldName, String[] fieldNames) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("UPDATE %s SET ", tableName));
        sqlBuilder.append(
                Stream.of(fieldNames)
                        .filter(fieldName -> !fieldName.equals(keyFieldName))
                        .map(fieldName -> String.format("%s = ?", fieldName))
                        .collect(Collectors.joining(","))
        );
        sqlBuilder.append(String.format(" WHERE %s = ?", keyFieldName));
        return sqlBuilder.toString();
    }

    private void feedUpdateStatement(Row row) {
        try {

            //here we bind all fields except technical id in SET clause
            int sqlCounter = 1;
            for (int i = 0; i < row.length(); i++) {
                if (i != keyIndex) {
                    DataType type = row.schema().fields()[i].dataType();
                    Object value = row.get(i);
                    bindParameter(preparedStatement, sqlCounter, type, value);
                    sqlCounter += 1;
                }
            }

            //here we bind technical id to where clause
            Object technicalIdValue = row.get(keyIndex);
            DataType type = row.schema().fields()[keyIndex].dataType();

            if (technicalIdValue == null) {
                throw new RuntimeException(String.format("value of field % cannot be null", functionalIdFieldName));
            }

            bindParameter(preparedStatement, sqlCounter, type, technicalIdValue);
            preparedStatement.addBatch();

        } catch (SQLException e) {
            throw new RuntimeException("Error while trying to update", e);
        }
    }

    private void bindParameter(PreparedStatement preparedStatement, int sqlCounter, DataType type, Object value) throws SQLException {
        if (type instanceof IntegerType) {
            if (value == null) {
                preparedStatement.setNull(sqlCounter, Types.INTEGER);
            } else {
                preparedStatement.setInt(sqlCounter, (Integer) value);
            }
        } else if (type instanceof DecimalType) {
            if (value == null) {
                preparedStatement.setNull(sqlCounter, Types.NUMERIC);
            } else {
                preparedStatement.setBigDecimal(sqlCounter, (BigDecimal) value);
            }
        } else if (type instanceof StringType) {
            if (value == null) {
                preparedStatement.setNull(sqlCounter, Types.VARCHAR);
            } else {
                preparedStatement.setString(sqlCounter, (String) value);
            }
        } else if (type instanceof BooleanType) {
            if (value == null) {
                preparedStatement.setNull(sqlCounter, Types.BOOLEAN);
            } else {
                preparedStatement.setBoolean(sqlCounter, (Boolean) value);
            }
        } else if (type instanceof DateType) {
            if (value == null) {
                preparedStatement.setNull(sqlCounter, Types.DATE);
            } else {
                preparedStatement.setDate(sqlCounter, (java.sql.Date) value);
            }
        } else if (type instanceof LongType) {
            if (value == null) {
                preparedStatement.setNull(sqlCounter, Types.BIGINT);
            } else {
                preparedStatement.setLong(sqlCounter, (Long) value);
            }
        } else {
            throw new RuntimeException(String.format("type %s not implemented in sql update", value.getClass().getCanonicalName()));
        }
    }

}
