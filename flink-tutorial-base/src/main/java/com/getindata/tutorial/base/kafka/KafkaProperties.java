package com.getindata.tutorial.base.kafka;

import java.util.Properties;

public class KafkaProperties {

    public static String getUsername() {
        // FIXME please return your username
        // return "lion";
        throw new UnsupportedOperationException("Please provide your user name.");
    }

    public static Properties getKafkaProperties() {
        final Properties properties = new Properties();
        // FIXME: uncomment if you are going to use docker
        // properties.setProperty("bootstrap.servers", "kafka:9092");
        // return properties;
        // FIXME: uncomment if you are going to use yarn cluster
        // properties.setProperty("bootstrap.servers", "flink-slave-01.c.getindata-training.internal:9092,flink-slave-02.c.getindata-training.internal:9092,flink-slave-03.c.getindata-training.internal:9092,flink-slave-04.c.getindata-training.internal:9092,flink-slave-05.c.getindata-training.internal:9092");
        // return properties;
        throw new UnsupportedOperationException("Please provide Kafka bootstrap servers.");
    }

    public static String getTopic(String user) {
        return "songs_" + user;
    }

    public static String getOutputTopic(String user) {
        return "statistics_" + user;
    }

    private KafkaProperties() {
    }
}
