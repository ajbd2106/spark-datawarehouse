package com.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Date;

public interface ISink extends Serializable {
    void insert(Dataset<Row> row);

    default void delete(Integer date, String inventoryDateFieldName){
        return;
    };

    /***
     * @param rowDataset set of data. Must contain only the data that should be updated and field id used in the 'where' condition (most commonly, the technical id)
     * @param functionalIdFieldName field name of the field used in the where condition (most commonly, the technical id)
     * @throws SQLException
     */
    void update(Dataset<Row> rowDataset, String functionalIdFieldName) throws SQLException;
}
