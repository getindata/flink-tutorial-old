package com.getindata.tutorial.base.kafka;


public class KafkaProperties {
    public static final String INPUT_TOPIC = "input";
    public static final String BOOTSTRAP_SERVERS = "kafka:9092";
    public static final String GROUP_ID = "tutorial-1";

    public static final String INPUT_AVRO_TOPIC = "input-avro";
    public static final String OUTPUT_AVRO_TOPIC = "output-avro";
    public static final String OUTPUT_SQL_AVRO_TOPIC = "output-sql-avro";
    public static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8082";
}
