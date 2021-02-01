package com.getindata.tutorial.base.generation;

import com.getindata.tutorial.base.input.SongsSource;
import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.io.Serializable;

public abstract class GenerationHelper<T> implements Serializable {
    private final String topic;
    private final KafkaSerializationSchema<T> serializer;

    protected GenerationHelper(String topic, KafkaSerializationSchema<T> serializer) {
        this.topic = topic;
        this.serializer = serializer;
    }

    public void run() throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<T> events = sEnv.addSource(new SongsSource())
                .map(this::map, TypeInformation.of(getConcreteClass()));

        events.addSink(
                new FlinkKafkaProducer<>(
                        topic,
                        serializer,
                        KafkaProperties.getKafkaProperties(),
                        FlinkKafkaProducer.Semantic.NONE
                )
        );

        sEnv.execute("Kafka producer");
    }

    protected abstract T map(SongEvent event);

    protected abstract Class<T> getConcreteClass();
}
