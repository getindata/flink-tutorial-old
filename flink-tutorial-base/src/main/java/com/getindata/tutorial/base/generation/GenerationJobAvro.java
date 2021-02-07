package com.getindata.tutorial.base.generation;

import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.model.solved.SongEventAvro;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;

public class GenerationJobAvro extends GenerationHelper<SongEventAvro> {
    protected GenerationJobAvro(String topic, KafkaSerializationSchema<SongEventAvro> serializer) {
        super(topic, serializer);
    }

    public static void main(String[] args) throws Exception {
        GenerationJobAvro job = new GenerationJobAvro(
                KafkaProperties.INPUT_AVRO_TOPIC,
                new KafkaSerializationSchemaWrapper<>(
                        KafkaProperties.INPUT_AVRO_TOPIC,
                        null,
                        false,
                        ConfluentRegistryAvroSerializationSchema.forSpecific(
                                SongEventAvro.class,
                                SongEventAvro.class.getSimpleName(),
                                KafkaProperties.SCHEMA_REGISTRY_URL)
                )
        );

        job.run();
    }

    @Override
    protected SongEventAvro map(SongEvent event) {
        return new SongEventAvro(
                event.getSongId(),
                event.getTimestamp(),
                event.getType().toString(),
                event.getUserId()
        );
    }

    @Override
    protected Class<SongEventAvro> getConcreteClass() {
        return SongEventAvro.class;
    }
}