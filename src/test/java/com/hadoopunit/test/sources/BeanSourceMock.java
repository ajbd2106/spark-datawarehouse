package com.hadoopunit.test.sources;

import com.source.ISource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class BeanSourceMock<T> implements ISource<T> {

    private final List<T> beanList;
    private final Class<T> sourceClass;

    public BeanSourceMock(Class<T> sourceClass, List<T> beanList) {
        this.beanList = beanList;
        this.sourceClass = sourceClass;
    }

    @Override
    public Dataset<Row> load(SparkSession sparkSession) {
        return sparkSession.createDataFrame(beanList, sourceClass);
    }

    @Override
    public Dataset<T> load(SparkSession sparkSession, Encoder<T> returnTypeEncoder) {
        return sparkSession.createDataset(beanList, returnTypeEncoder);
    }

    public void clear(){
        beanList.clear();
    }
}
