package com.sink;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PostGreSqlDeleteAction {

    private final String tableName;
    private final String inventoryDateFieldName;
    private final Integer inventoryDate;
    private final String sql;


    public PostGreSqlDeleteAction(String tableName, String inventoryDateFieldName, Integer inventoryDate) {
        this.tableName = tableName;
        this.inventoryDateFieldName = inventoryDateFieldName;
        this.inventoryDate = inventoryDate;
        this.sql = String.format("DELETE FROM %s WHERE %s = %d", tableName, inventoryDateFieldName, inventoryDate);
    }

    public void execute(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        Statement stmt = connection.createStatement();
        stmt.executeUpdate(sql);
        connection.commit();
    }
}
