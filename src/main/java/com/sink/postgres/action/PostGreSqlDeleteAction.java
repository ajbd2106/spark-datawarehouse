package com.sink.postgres.action;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PostGreSqlDeleteAction implements Serializable {

    private final String sql;

    public PostGreSqlDeleteAction(String tableName, String inventoryDateFieldName, Integer inventoryDate) {
        this.sql = String.format("DELETE FROM %s WHERE %s = %d", tableName, inventoryDateFieldName, inventoryDate);
    }

    public void execute(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
            connection.commit();
        }
    }
}
