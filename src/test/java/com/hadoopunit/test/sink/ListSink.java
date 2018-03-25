package com.hadoopunit.test.sink;

import com.sink.ISink;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Getter
public class ListSink implements ISink {

    private List<Row> insertResult = new ArrayList<>();
    private List<Row> updateResult = new ArrayList<>();
    private String updateTableName = StringUtils.EMPTY;
    private String updateKeyFieldName = StringUtils.EMPTY;

    @Override
    public void insert(Dataset<Row> row) {
        this.insertResult = row.collectAsList();
    }

    @Override
    public void delete(Integer date, String inventoryDateFieldName) {

    }

    @Override
    public void update(Dataset<Row> rowDataset, String functionalIdFieldName) throws SQLException {

    }

    public void clear() {
        insertResult.clear();
        updateResult.clear();
        updateTableName = StringUtils.EMPTY;
        updateKeyFieldName = StringUtils.EMPTY;
    }
}
