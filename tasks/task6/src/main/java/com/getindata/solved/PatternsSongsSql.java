package com.getindata.solved;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class PatternsSongsSql {

    public static void main(String[] args) {
        final EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        createInMemorySourceTable(tableEnv);
        tableEnv.executeSql("SELECT * FROM songs").print();

        tableEnv.executeSql("SELECT * \n" +
                "FROM songs\n" +
                "    MATCH_RECOGNIZE (\n" +
                "      PARTITION BY userid\n" +
                "      ORDER BY ts\n" +
                "      MEASURES\n" +
                "        FIRST(A.song) AS song,\n" +
                "        FIRST(A.listeningid) AS fli,\n" +
                "        LAST(A.listeningid) AS lli\n" +
                " AFTER MATCH SKIP PAST LAST ROW \n" +
                "      PATTERN (A B{2,} C)\n" +
                "      DEFINE\n" +
                "        A AS true,\n" +
                "        B AS B.song = A.song\n" +
                "    ) \n").print();
    }

    private static void createInMemorySourceTable(TableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE VIEW songs as  (\n" +
                "SELECT *\n" +
                "  FROM (VALUES (1, 'a', 1, PROCTIME())\n" +
                "             , (1, 'b', 2, PROCTIME())\n" +
                "             , (1, 'a', 3, PROCTIME())\n" +
                "             , (1, 'a', 4, PROCTIME())\n" +
                "             , (1, 'c', 5, PROCTIME())\n" +
                "             , (1, 'a', 6, PROCTIME())\n" +
                "             , (1, 'a', 7, PROCTIME())\n" +
                "             , (1, 'a', 8, PROCTIME())\n" +
                "             , (1, 'a', 9, PROCTIME())\n" +
                "             , (1, 'd', 10, PROCTIME())\n" +
                "       ) t1 (userid, song, listeningid, ts))"
        );
    }

}