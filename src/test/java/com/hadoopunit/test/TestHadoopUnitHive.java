package com.hadoopunit.test;

import com.spark.SparkSessionFactory;
import com.starschema.dimension.DimensionBean;
import com.starschema.columnSelector.DimensionColumnSelector;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;

@Slf4j
public class TestHadoopUnitHive implements Serializable {

    private static Configuration configuration;

    @BeforeClass
    public static void setUp() throws ConfigurationException, NotFoundServiceException, InterruptedException {
        configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);

        HadoopBootstrap.INSTANCE.add(Component.ZOOKEEPER);
        HadoopBootstrap.INSTANCE.add(Component.HDFS);
        HadoopBootstrap.INSTANCE.add(Component.HIVEMETA);
        HadoopBootstrap.INSTANCE.add(Component.HIVESERVER2);
        HadoopBootstrap.INSTANCE.startAll();

    }

    @Test
    public void spark_should_create_table() {

        SparkSession spark = SparkSessionFactory.createHiveSparkSession("local[*]", "test");

        Dataset<Row> row = spark.read().json("src/main/resources/stagingTable.json")
                .select(DimensionColumnSelector.getStageColumns(DimensionBean.class));

        spark.sql("CREATE TABLE default.dimension (functionalId STRING, name STRING, lookupType String, SCD1CheckSum String, SCD2CheckSum String) STORED AS ORC");

        row.write().insertInto("default.dimension");

        Dataset<Row> selectRows = spark.sql("SELECT functionalId, name, lookupType FROM default.dimension");

        spark.sql("UPDATE default.dimension SET functionalId = 2");
        System.out.println(selectRows.count());

        spark.close();


    }
}
