package com.getindata;

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
                "      PARTITION BY ...\n" +
                "      ORDER BY ...\n" +
                "      MEASURES\n" +
                "        ...\n" +
                " AFTER MATCH SKIP PAST LAST ROW \n" +
                "      PATTERN ...\n" +
                "      DEFINE\n" +
                "        ...\n" +
                "    ) \n").print();
    }

    private static void createInMemorySourceTable(TableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE VIEW songs as  (\n" +
                "SELECT *\n" +
                "  FROM (VALUES ...\n" +
                "       ) t1 (userid, song, listeningid, ts))"
        );
    }

}