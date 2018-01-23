package com.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class RowDataSetSource implements ISource<Row> {

    private final Dataset<Row> dataSet;

    public RowDataSetSource(Dataset<Row> dataset) {
        this.dataSet = dataset;
    }

    @Override
    public Dataset<Row> load(SparkSession sparkSession) {
        return dataSet;
    }

    @Override
    public Dataset<Row> load(SparkSession sparkSession, Encoder<Row> returnTypeEncoder) {
        throw new NotImplementedException();
    }
}
