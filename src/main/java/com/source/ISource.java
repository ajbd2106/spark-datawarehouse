package com.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public interface ISource<T> extends Serializable {
    Dataset<Row> load(SparkSession sparkSession);
    Dataset<T> load(SparkSession sparkSession, Encoder<T> returnTypeEncoder);
}
