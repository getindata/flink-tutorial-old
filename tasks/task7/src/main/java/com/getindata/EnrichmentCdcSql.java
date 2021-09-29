package com.getindata;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.getindata.model.SongCdcEvent;
import com.getindata.serde.SongDebeziumDeserializationSchema;
import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.solved.SongEventAvro;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.time.Duration;

public class EnrichmentCdcSql {

    /*
     * Prerequisites:
     * * Add "127.0.0.1    mysql" to /etc/hosts
     */

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<SongEventAvro> events = env.addSource(getKafkaEventsSource());
        final DataStream<SongCdcEvent> songsMetadata = env.addSource(getSongMetadataSource());

        events.print();
        songsMetadata.print();

        env.execute("Enrichment CDC");
    }

    private static FlinkKafkaConsumerBase<SongEventAvro> getKafkaEventsSource() {
        return new FlinkKafkaConsumer<>(
                KafkaProperties.INPUT_AVRO_TOPIC,
                ConfluentRegistryAvroDeserializationSchema.forSpecific(
                        SongEventAvro.class,
                        KafkaProperties.SCHEMA_REGISTRY_URL
                ),
                KafkaProperties.getKafkaProperties()
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<SongEventAvro>forBoundedOutOfOrderness(Duration.ofMinutes(5L))
                        .withTimestampAssigner(new TimestampAssignerSupplier<SongEventAvro>() {
                            @Override
                            public TimestampAssigner<SongEventAvro> createTimestampAssigner(Context context) {
                                return (songEventAvro, l) -> songEventAvro.getTimestamp();
                            }
                        })
        );
    }

    private static DebeziumSourceFunction<SongCdcEvent> getSongMetadataSource() {
        return MySQLSource.<SongCdcEvent>builder()
                .hostname("mysql")
                .port(3306)
                .databaseList("music_streaming")
                .tableList("music_streaming.songs")
                .username("debezium")
                .password("dbz")
                .deserializer(new SongDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
    }

}