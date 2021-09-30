package com.getindata;


import com.getindata.model.SongCdcEvent;
import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.solved.SongEventAvro;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;

public class EnrichmentCdcSql {

    /*
     * Prerequisites:
     * * Add "127.0.0.1    mysql" to /etc/hosts
     * * Run docker-compose build
     *
     * How to login to mysql?
     *      docker exec -it docker_mysql_1 bash
     *      mysql -u mysqluser -pmysqlpw
     *
     *      use music_streaming;
     *      select * from songs;
     */

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        final DataStream<SongEventAvro> events = env.addSource(getKafkaEventsSource());
        final DataStream<SongCdcEvent> cdcEvents = getSongCdcEvents(tenv);

        // TODO: keyBy both streams, connect them and apply KeyedCoProcessFunction which implements low-level join.

        cdcEvents.print();

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

    private static DataStream<SongCdcEvent> getSongCdcEvents(StreamTableEnvironment tenv) {
        final Table songsBinlog = getSongsBinlogTable(tenv);

        return tenv.toChangelogStream(songsBinlog)
                .flatMap(new MapToPojo());
    }

    private static Table getSongsBinlogTable(StreamTableEnvironment tenv) {
        tenv.executeSql("CREATE TABLE songs_binlog (\n" +
                "   id BIGINT NOT NULL,\n" +
                "   author STRING NOT NULL,\n" +
                "   title STRING NOT NULL,\n" +
                "   last_updated TIMESTAMP(3) NOT NULL,\n" +
                "   WATERMARK FOR last_updated AS last_updated " +
                ") WITH (\n" +
                "   'connector' = 'mysql-cdc',\n" +
                "   'hostname' = 'mysql',\n" +
                "   'port' = '3306',\n" +
                "   'username' = 'debezium',\n" +
                "   'password' = 'dbz',\n" +
                "   'database-name' = 'music_streaming',\n" +
                "   'table-name' = 'songs'\n" +
                ")");

        return tenv.sqlQuery("SELECT * FROM songs_binlog");
    }

    private static class MapToPojo implements FlatMapFunction<Row, SongCdcEvent> {


        @Override
        public void flatMap(Row row, Collector<SongCdcEvent> collector) {
            final SongCdcEvent.Operation operation;
            if (row.getKind().equals(RowKind.INSERT)) {
                operation = SongCdcEvent.Operation.INSERT;
            } else if (row.getKind().equals(RowKind.DELETE)) {
                operation = SongCdcEvent.Operation.DELETE;
            } else if (row.getKind().equals(RowKind.UPDATE_AFTER)) {
                operation = SongCdcEvent.Operation.UPDATE;
            } else {
                operation = null;
            }

            if (operation != null) {
                collector.collect(new SongCdcEvent(
                                operation,
                                ((LocalDateTime) row.getField(3)).getSecond() * 1000,
                                (Long) row.getField(0),
                                (String) row.getField(1),
                                (String) row.getField(2)
                        )
                );
            }
        }

    }

}