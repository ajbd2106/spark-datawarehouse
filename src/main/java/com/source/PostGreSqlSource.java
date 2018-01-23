package com.source;

import com.spark.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PostGreSqlSource<T> implements ISource<T> {

    private final String tableName;

    public PostGreSqlSource(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public Dataset<Row> load(SparkSession sparkSession){
        return sparkSession.read()
                .format("jdbc")
                .options(SparkSessionFactory.getPostGreSqlOptions())
                .option("dbtable", tableName)
                .load();
    }

    @Override
    public Dataset<T> load(SparkSession sparkSession, Encoder<T> returnTypeEncoder){
        return load(sparkSession).as(returnTypeEncoder);
    }

}
