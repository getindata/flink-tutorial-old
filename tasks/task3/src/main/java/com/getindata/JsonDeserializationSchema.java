package com.getindata;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

class JsonDeserializationSchema<T> implements KafkaDeserializationSchema<T> {

    private final Class<T> outputClass;
    private final ObjectMapper objectMapper;

    JsonDeserializationSchema(Class<T> outputClass) {
        this.outputClass = outputClass;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return objectMapper.readValue(record.value(), outputClass);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(outputClass);
    }
}
