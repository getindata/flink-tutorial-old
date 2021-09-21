package com.getindata;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class EnrichSongsSqlTest {

    private static final int TITLE_COLUMN_POSITION = 4;
    private static final int AUTHOR_COLUMN_POSITION = 5;
    private static final int LENGTH_COLUMN_POSITION = 6;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    // It is recommended to always test your pipelines locally with a parallelism > 1 to identify bugs
                    // which only surface for the pipelines executed in parallel.
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build()
    );

    @Test
    public void shouldEnrichValidEvent() {
        // given: execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // and: songs table
        DataStream<Row> rows = env.fromElements(
                Row.of(1L, 1000L, "PLAY", 10L)
        );
        dataStreamToTable(tableEnv, rows);

        // when
        // TODO

        // then
        List<Row> enrichedSongs = collectRowsFromTable(tableEnv, "enriched_songs");
        assertEquals(1, enrichedSongs.size());
        assertEquals("Yellow Submarine", enrichedSongs.get(0).getField(TITLE_COLUMN_POSITION));
        assertEquals("The Beatles", enrichedSongs.get(0).getField(AUTHOR_COLUMN_POSITION));
        assertEquals(160000, enrichedSongs.get(0).getField(LENGTH_COLUMN_POSITION));
    }

    @Test
    public void shouldNotEnrichValidEvent() {
        // given: execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // and: songs table
        DataStream<Row> rows = env.fromElements(
                Row.of(999999L, 1000L, "PLAY", 10L)
        );
        dataStreamToTable(tableEnv, rows);

        // when
        // TODO

        // then
        List<Row> enrichedSongs = collectRowsFromTable(tableEnv, "enriched_songs");
        assertEquals(1, enrichedSongs.size());
        assertNull(enrichedSongs.get(0).getField(TITLE_COLUMN_POSITION));
        assertNull(enrichedSongs.get(0).getField(AUTHOR_COLUMN_POSITION));
        assertNull(enrichedSongs.get(0).getField(LENGTH_COLUMN_POSITION));
    }


    private void dataStreamToTable(StreamTableEnvironment tableEnv, DataStream<Row> rows) {
        Table inputTable = tableEnv.fromDataStream(rows, $("songId"), $("timestamp"), $("type"), $("userId"));
        tableEnv.createTemporaryView("songs", inputTable);
    }


    private List<Row> collectRowsFromTable(StreamTableEnvironment tableEnv, String tableName) {
        List<Row> rows = new ArrayList<>();
        CloseableIterator<Row> collect = tableEnv.sqlQuery("SELECT * FROM " + tableName).execute().collect();
        while (collect.hasNext()) {
            rows.add(collect.next());
        }
        return rows;
    }

}