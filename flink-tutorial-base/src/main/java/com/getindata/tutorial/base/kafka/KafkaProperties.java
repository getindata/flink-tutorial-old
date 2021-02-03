package com.getindata.tutorial.base.kafka;

import java.util.Properties;

public class KafkaProperties {
    public static final String INPUT_TOPIC = "input";

    public static final String INPUT_AVRO_TOPIC = "input-avro";
    public static final String OUTPUT_AVRO_TOPIC = "output-avro";
    public static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8082";

    public static Properties getKafkaProperties() {
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        return properties;
    }
}
