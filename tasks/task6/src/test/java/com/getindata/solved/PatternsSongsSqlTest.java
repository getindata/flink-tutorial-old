package com.getindata.solved;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.getindata.solved.PatternsSongsSql.pattern;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PatternsSongsSqlTest {
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
    public void shouldNotFindPattern() {
        // given: execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // and: songs table
        tableEnv.executeSql("CREATE VIEW songs as  (\n" +
                "SELECT *\n" +
                "  FROM (VALUES (1, 'Angie', 1, PROCTIME())\n" +
                "             , (1, 'Billie Jean', 2, PROCTIME())\n" +
                "       ) t1 (userid, song, listeningid, ts))"
        );

        // when
        TableResult res = pattern(tableEnv);

        // then
        List<Row> foundSongs = collectRowsFromTable(tableEnv, res);
        assertEquals(0, foundSongs.size());
    }

    @Test
    public void shouldFindPattern() {
        // given: execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // and: songs table
        tableEnv.executeSql("CREATE VIEW songs as  (\n" +
                "SELECT *\n" +
                "  FROM (VALUES (1, 'Angie', 1, PROCTIME())\n" +
                "             , (1, 'Billie Jean', 2, PROCTIME())\n" +
                "             , (1, 'Billie Jean', 3, PROCTIME())\n" +
                "             , (1, 'Billie Jean', 4, PROCTIME())\n" +
                "             , (1, 'Layla', 5, PROCTIME())\n" +
                "       ) t1 (userid, song, listeningid, ts))"
        );

        // when
        TableResult res = pattern(tableEnv);

        // then
        List<Row> foundSongs = collectRowsFromTable(tableEnv, res);
        assertEquals(1, foundSongs.size());
        assertEquals("Billie Jean", foundSongs.get(0).getField("song"));
        assertEquals(2, foundSongs.get(0).getField("fli"));
        assertEquals(4, foundSongs.get(0).getField("lli"));
    }

    private List<Row> collectRowsFromTable(StreamTableEnvironment tableEnv, TableResult res) {
        List<Row> rows = new ArrayList<>();
        CloseableIterator<Row> collect = res.collect();
        while (collect.hasNext()) {
            rows.add(collect.next());
        }
        return rows;
    }

}