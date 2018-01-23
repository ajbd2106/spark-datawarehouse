package com.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SourceFactory {

    public static <T> ISource<T> createPostGreSqlSource(String tableName){
         return new PostGreSqlSource(tableName);
    }

    public static ISource<Row> createRowDataSetSource(Dataset<Row> dataset){
        return new RowDataSetSource(dataset);
    }
}
