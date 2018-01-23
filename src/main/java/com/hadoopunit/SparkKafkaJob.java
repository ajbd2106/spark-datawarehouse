package com.hadoopunit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class SparkKafkaJob implements Serializable {

    private final JavaStreamingContext scc;
    private String zkString;
    private String topic;

    public SparkKafkaJob(JavaStreamingContext scc) {
        this.scc = scc;

    }

    public void setZkString(String zkString) {
        this.zkString = zkString;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void run() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", zkString);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "groupId");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList(topic);

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                scc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );


        JavaDStream<String> messages = stream.map(r -> r.value());

        messages.foreachRDD(r -> {
            r.foreachPartition(
                    partition -> partition.forEachRemaining(
                            currentString -> System.out.println(currentString)
                    )
            );
        });
    }
}
