package com.getindata.solved;

import com.getindata.tutorial.base.enrichmennt.EnrichmentService;
import com.getindata.tutorial.base.kafka.KafkaProperties;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

public class EnrichSongsSql {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final TableEnvironment tableEnv = StreamTableEnvironment.create(env);

        createKafkaSourceTable(tableEnv);
        createKafkaSinkTable(tableEnv);

        enrichment(tableEnv);

        // Write enriched songs into kafka (title, author, long are non-null when enrichment succeeded).
        tableEnv.executeSql("INSERT INTO enriched_songs_sink SELECT * FROM enriched_songs WHERE title IS NOT NULL");
        // Print not enriched songs into stdout (title, author, long are null when enrichment failed).
        tableEnv.executeSql("SELECT * FROM enriched_songs WHERE title IS NULL").print();
    }

    @VisibleForTesting
    static void enrichment(TableEnvironment tableEnv) {
        tableEnv.createTemporarySystemFunction("Enrich", EnrichmentFunction.class);
        tableEnv.executeSql(
                "CREATE VIEW enriched_songs AS SELECT\n" +
                        "   s.songId,\n" +
                        "   s.`timestamp`,\n" +
                        "   s.type,\n" +
                        "   s.userId,\n" +
                        "   e.f0 as title,\n" +
                        "   e.f1 as author,\n" +
                        "   e.f2 as length\n" +
                        "FROM\n" +
                        "   songs AS s\n" +
                        "   LEFT JOIN LATERAL TABLE(ENRICH(songId)) AS e ON TRUE"
        );
    }

    private static void createKafkaSourceTable(TableEnvironment tableEnv) {
        tableEnv.executeSql(
                "CREATE TABLE songs (" +
                        "   songId BIGINT, " +
                        "   `timestamp` TIMESTAMP(3), " +
                        "   type STRING, " +
                        "   userId INT" +
                        ") WITH (" +
                        "   'connector' = 'kafka'," +
                        "   'topic' = '" + KafkaProperties.INPUT_AVRO_TOPIC + "'," +
                        "   'properties.group.id' = 'flink_tutorial'," +
                        "   'properties.bootstrap.servers' = 'kafka:9092'," +
                        "   'scan.startup.mode' = 'earliest-offset'," +
                        "   'value.format' = 'avro-confluent'," +
                        "   'value.avro-confluent.schema-registry.url' = 'http://schema-registry:8082'" +
                        ")"
        );
    }

    private static void createKafkaSinkTable(TableEnvironment tableEnv) {
        tableEnv.executeSql(
                "CREATE TABLE enriched_songs_sink (\n" +
                        "songId BIGINT,\n" +
                        "   `timestamp` TIMESTAMP(3),\n" +
                        "   type STRING,\n" +
                        "   userId INT,\n" +
                        "   title STRING,\n" +
                        "   author STRING,\n" +
                        "   length INT\n" +
                        ") WITH (\n" +
                        "   'connector' = 'kafka',\n" +
                        "   'topic' = '" + KafkaProperties.OUTPUT_SQL_AVRO_TOPIC + "',\n" +
                        "   'properties.bootstrap.servers' = 'kafka:9092',\n" +
                        "   'value.format' = 'avro-confluent',\n" +
                        "   'value.avro-confluent.schema-registry.url' = 'http://schema-registry:8082',\n" +
                        "   'value.avro-confluent.schema-registry.subject' = 'EnrichedSongEventAvro'\n" +
                        ")"
        );
    }


    public static class EnrichmentFunction extends TableFunction<Tuple3<String, String, Integer>> {

        private transient EnrichmentService service;

        @Override
        public void open(FunctionContext context) {
            this.service = new EnrichmentService();
        }

        public void eval(Long songId) {
            service.getSongById(songId).ifPresent(song ->
                    collect(Tuple3.of(song.getName(), song.getAuthor(), song.getLength()))
            );
        }
    }

}
