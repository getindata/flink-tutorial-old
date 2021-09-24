package com.getindata;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class PatternsSongsSql {

    public static void main(String[] args) {
        final EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        createInMemorySourceTable(tableEnv);
        tableEnv.executeSql("SELECT * FROM songs").print();
        pattern(tableEnv).print();
    }

    @VisibleForTesting
    static TableResult pattern(TableEnvironment tableEnv) {
        return tableEnv.executeSql("SELECT * \n" +
                "FROM songs\n" +
                "    MATCH_RECOGNIZE (\n" +
                "      PARTITION BY ...\n" +
                "      ORDER BY ...\n" +
                "      MEASURES\n" +
                "        ... AS song,\n" +
                "        ... AS fli,\n" +
                "        ... AS lli\n" +
                "      PATTERN ...\n" +
                "      DEFINE\n" +
                "        ...\n" +
                "    ) \n");
    }

    private static void createInMemorySourceTable(TableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE VIEW songs as  (\n" +
                "SELECT *\n" +
                "  FROM (VALUES (...)\n" +
                "       ) t1 (userid, song, listeningid, ts))"
        );
    }

}