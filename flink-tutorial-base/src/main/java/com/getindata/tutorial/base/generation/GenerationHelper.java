package com.getindata.tutorial.base.generation;

import com.getindata.tutorial.base.input.SongsSource;
import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

public abstract class GenerationHelper<T> implements Serializable {
    private final String topic;
    private final KafkaRecordSerializationSchema<T> serializer;

    protected GenerationHelper(String topic, KafkaRecordSerializationSchema<T> serializer) {
        this.topic = topic;
        this.serializer = serializer;
    }

    public void run() throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<T> events = sEnv.addSource(new SongsSource())
                .map(this::map, TypeInformation.of(getConcreteClass()));

        final KafkaSink<T> sink = KafkaSink.<T>builder()
                .setBootstrapServers(KafkaProperties.BOOTSTRAP_SERVERS)
                .setRecordSerializer(serializer)
                .build();

        events.sinkTo(sink);

        sEnv.execute("Kafka producer");
    }

    protected abstract T map(SongEvent event);

    protected abstract Class<T> getConcreteClass();
}
