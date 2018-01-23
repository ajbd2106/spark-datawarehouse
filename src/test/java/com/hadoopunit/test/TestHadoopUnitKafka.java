package com.hadoopunit.test;

import com.hadoopunit.SparkKafkaJob;
import fr.jetoile.hadoopunit.Component;
import fr.jetoile.hadoopunit.HadoopBootstrap;
import fr.jetoile.hadoopunit.HadoopUnitConfig;
import fr.jetoile.hadoopunit.exception.NotFoundServiceException;
import fr.jetoile.hadoopunit.test.kafka.KafkaProducerUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;

public class TestHadoopUnitKafka implements Serializable {
	private static Configuration configuration;

    @BeforeClass
    public static void setUp() throws ConfigurationException, NotFoundServiceException, InterruptedException {
		configuration = new PropertiesConfiguration(HadoopUnitConfig.DEFAULT_PROPS_FILE);

		HadoopBootstrap.INSTANCE.add(Component.ZOOKEEPER);
        HadoopBootstrap.INSTANCE.add(Component.KAFKA);
        HadoopBootstrap.INSTANCE.add(Component.HDFS);
        HadoopBootstrap.INSTANCE.add(Component.HBASE);
        HadoopBootstrap.INSTANCE.add(Component.HIVEMETA);
        HadoopBootstrap.INSTANCE.add(Component.HIVESERVER2);
		HadoopBootstrap.INSTANCE.startAll();

    }

    @Test
    public void spark_should_read_kafka() throws InterruptedException {

        for (int i = 0; i < 10; i++) {
            String payload = generateMessage(i);
            KafkaProducerUtils.INSTANCE.produceMessages(configuration.getString(HadoopUnitConfig.KAFKA_TEST_TOPIC_KEY),
                    String.valueOf(i), payload);
            System.out.println("toto");
        }

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaStreamingContext scc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        SparkKafkaJob sparkKafkaJob = new SparkKafkaJob(scc);
        sparkKafkaJob.setTopic("testtopic");
        sparkKafkaJob.setZkString(configuration.getString(HadoopUnitConfig.KAFKA_HOSTNAME_KEY) + ":" + configuration.getInt(HadoopUnitConfig.KAFKA_PORT_KEY));

        sparkKafkaJob.run();

        scc.start();
        scc.awaitTermination();


    }

    private String generateMessage(int i) {
        return String.valueOf(i);
    }

}
