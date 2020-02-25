package com.getindata.tutorial.base.input;

import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class SongEventSerializationSchema implements KafkaSerializationSchema<SongEvent> {

    private final ObjectMapper objectMapper;
    private final String topic;

    public SongEventSerializationSchema(String topic) {
        this.topic = topic;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(SongEvent element, @Nullable Long timestamp) {
        try {
            return new ProducerRecord<>(topic, objectMapper.writeValueAsBytes(element));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize song event.", e);
        }
    }

}
