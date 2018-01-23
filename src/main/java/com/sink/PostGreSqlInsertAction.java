package com.sink;

import com.csv.CsvStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class PostGreSqlInsertAction {

    private final String SEPARATOR = "|";
    private final String tableName;
    private final StringBuilder lines;
    private final String header;

    public PostGreSqlInsertAction(String tableName, String[] columns) {
        this.tableName = tableName;
        this.header = StringUtils.join(columns, ",");
        this.lines = new StringBuilder();
    }

    public void process(Row row) {
        lines.append(row.mkString(StringUtils.EMPTY, SEPARATOR, "\n"));
    }

    public void execute(Connection connection) throws SQLException {
        String content = lines.toString();

        if (StringUtils.isNotEmpty(content)) {
            CsvStream csvStream = new CsvStream(content);
            CopyManager cm = new CopyManager((BaseConnection) connection);
            try {
                cm.copyIn(String.format("COPY %s (%s) FROM STDIN WITH DELIMITER '%s'", tableName, header, SEPARATOR),
                        csvStream.getContent());
            } catch (IOException e) {
                throw new RuntimeException(String.format("Error while inserting data for table %s", tableName), e);
            }
        }

    }
}
