package com.sink.postgres;

import com.postgresql.PostGreSqlConfig;
import com.sink.ISink;
import com.sink.postgres.action.PostGreSqlInsertAction;
import com.sink.postgres.action.PostGreSqlUpdateAction;
import com.sink.postgres.command.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
public class PostGreSqlSink implements ISink {

    private final String databaseUrl;
    private final String user;
    private final String password;
    private final String tableName;
    private final String tableNameToClone;
    private final InsertMode insertMode;

    public PostGreSqlSink(String tableName, String tableNameToClone, InsertMode insertMode) {
        this.databaseUrl = PostGreSqlConfig.getInstance().getUrl();
        this.user = PostGreSqlConfig.getInstance().getUser();
        this.password = PostGreSqlConfig.getInstance().getPassword();
        this.tableName = tableName;
        this.tableNameToClone = tableNameToClone;
        this.insertMode = insertMode;
    }

    public PostGreSqlSink(String tableName, InsertMode insertMode) {
        this(tableName, null, insertMode);
    }

    private void executeCommand(PostGreCommand postGreCommand) {
        try (Connection connection = DriverManager.getConnection(databaseUrl, user, password);
             Statement stmt = connection.createStatement()) {
            stmt.execute(postGreCommand.getCommand());
        } catch (SQLException e) {
            String errorMsg = String.format(postGreCommand.getErrorMessage());
            log.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }

    @Override
    public void dropTable(String name) {
        executeCommand(new DropTableCommand(name));
    }

    @Override
    public void renameTable(String oldName, String newName) {
        executeCommand(new RenameTableCommand(oldName, newName));
    }

    @Override
    public void delete(Integer date, String inventoryDateFieldName) {
        executeCommand(new DeleteByInventoryDateCommand(tableName, inventoryDateFieldName, date));
    }

    private void cloneTableIfNotExists() {
        executeCommand(new CloneTableCommand(tableName, tableNameToClone));
    }

    private void truncateTable() {
        executeCommand(new TruncateTableCommand(tableName));
    }

    @Override
    public void insert(Dataset<Row> dataset) {

        if (tableNameToClone != null) {
            cloneTableIfNotExists();
        }

        if (insertMode.equals(InsertMode.OVERWRITE)) {
            truncateTable();
        }

        PostGreSqlInsertAction insertAction = new PostGreSqlInsertAction(tableName, dataset.columns());

        dataset.foreachPartition(rowIterator -> {
            if (rowIterator.hasNext()) {
                rowIterator.forEachRemaining(insertAction::process);
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

                            rowIterator.forEachRemaining(updateAction::process);

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


}
