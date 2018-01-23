package com.hadoopunit.test.spark;

import com.spark.SparkSessionFactory;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;

@Getter
public class TestSparkSession {

    SparkSession sparkSession = SparkSessionFactory.createClassicSparkSession("local[*]", "Java Spark SQL basic example");

    private TestSparkSession() {
    }


    private static TestSparkSession INSTANCE = new TestSparkSession();


    public static TestSparkSession getInstance() {
        return INSTANCE;
    }

}
