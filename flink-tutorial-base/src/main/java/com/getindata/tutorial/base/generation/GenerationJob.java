package com.getindata.tutorial.base.generation;

import com.getindata.tutorial.base.input.SongEventSerializationSchema;
import com.getindata.tutorial.base.input.SongsSource;
import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;

public class GenerationJob {

    private static final String[] USERS = new String[]{
            "alpaca",
            "bat",
            "bear",
            "bison",
            "bull",
            "camel",
            "deer",
            "duck",
            "eagle",
            "fenek",
            "ferret",
            "fox",
            "gazelle",
            "hornet",
            "koala",
            "lama",
            "lemur",
            "lion",
            "monkey",
            "narwhal",
            "owl",
            "panda",
            "puma",
            "shark",
            "snake",
            "tiger",
            "wolf",
            "zebra"
    };

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<SongEvent> events = sEnv.addSource(new SongsSource());

        for (String user : USERS) {
            String topic = KafkaProperties.getTopic(user);
            events.addSink(
                    new FlinkKafkaProducer<>(
                            topic,
                            new SongEventSerializationSchema(topic),
                            KafkaProperties.getKafkaProperties(),
                            Semantic.NONE
                    )
            );
        }

        sEnv.execute("Kafka producer");
    }

}
