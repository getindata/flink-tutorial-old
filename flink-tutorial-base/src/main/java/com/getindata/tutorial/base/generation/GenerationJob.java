package com.getindata.tutorial.base.generation;

import com.getindata.tutorial.base.input.SongEventSerializationSchema;
import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

public class GenerationJob extends GenerationHelper<SongEvent> {
    protected GenerationJob(String topic, KafkaSerializationSchema<SongEvent> serializer) {
        super(topic, serializer);
    }

    public static void main(String[] args) throws Exception {
        GenerationJob job = new GenerationJob(
                KafkaProperties.INPUT_TOPIC,
                new SongEventSerializationSchema(KafkaProperties.INPUT_TOPIC)
        );

        job.run();
    }

    @Override
    protected SongEvent map(SongEvent event) {
        return event;
    }

    @Override
    protected Class<SongEvent> getConcreteClass() {
        return SongEvent.class;
    }
}