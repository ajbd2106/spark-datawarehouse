package com.sink;

import com.postgresql.PostGreSqlConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Slf4j
public class PostGreSqlSink implements ISink {

    private final String databaseUrl;
    private final String user;
    private final String password;
    private final String tableName;

    public PostGreSqlSink(String tableName) {
        this.databaseUrl = PostGreSqlConfig.getInstance().getUrl();
        this.user = PostGreSqlConfig.getInstance().getUser();
        this.password = PostGreSqlConfig.getInstance().getPassword();
        this.tableName = tableName;
    }

    @Override
    public void insert(Dataset<Row> dataset) {

        PostGreSqlInsertAction insertAction = new PostGreSqlInsertAction(tableName, dataset.columns());

        dataset.foreachPartition(rowIterator -> {
            if (rowIterator.hasNext()) {
                rowIterator.forEachRemaining(row -> insertAction.process(row));
                try (Connection connection = DriverManager.getConnection(databaseUrl, user, password)) {
                    insertAction.execute(connection);
                }
            }
        });
    }

    @Override
    public void update(Dataset<Row> rowDataset, String functionalIdFieldName) {
        PostGreSqlUpdateAction updateAction = new PostGreSqlUpdateAction(tableName, rowDataset.schema(), functionalIdFieldName);

        rowDataset.foreachPartition(
                rowIterator -> {
                    if (rowIterator.hasNext()) {
                        try (Connection connection = DriverManager.getConnection(databaseUrl, user, password)) {
                            updateAction.prepare(connection);
                            rowIterator.forEachRemaining(row -> updateAction.process(row));
                            try {
                                updateAction.execute(connection);
                                updateAction.close();
                            } catch (SQLException e) {
                                connection.rollback();
                                //closing connection here, because exception is thrown just after
                                connection.close();
                                String errorMessage = String.format("Error while updating table %s", tableName);
                                log.error(errorMessage, e);
                                throw new RuntimeException(errorMessage, e);
                            }
                        }
                    }
                }
        );
    }

    @Override
    public void delete(Integer date, String inventoryDateFieldName) {

        PostGreSqlDeleteAction deleteAction = new PostGreSqlDeleteAction(tableName, inventoryDateFieldName, date);

        try (Connection connection = DriverManager.getConnection(databaseUrl, user, password)) {
            try {
                deleteAction.execute(connection);
            } catch (SQLException e) {
                connection.rollback();
                //closing connection here, because exception is thrown just after
                connection.close();
                String errorMessage = String.format("Error while deleting table %s", tableName);
                log.error(errorMessage, e);
                throw new RuntimeException(errorMessage, e);
            }
        } catch (SQLException e) {
            log.error("Error while opening connection", e);
            throw new RuntimeException("Error while opening connection", e);
        }
    }

}
