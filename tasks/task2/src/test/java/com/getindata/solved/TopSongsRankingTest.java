package com.getindata.solved;

import com.getindata.solved.TopSongsRanking.SongsRanking;
import com.getindata.tutorial.base.model.EnrichedSongEvent;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.getindata.tutorial.base.model.TestDataBuilders.aSong;
import static com.getindata.tutorial.base.model.TestDataBuilders.aSongEvent;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TopSongsRankingTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    // It is recommended to always test your pipelines locally with a parallelism > 1 to identify bugs
                    // which only surface for the pipelines executed in parallel.
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build()
    );

    @BeforeEach
    void setup() {
        CollectSink.values.clear();
    }

    @Test
    void shouldReturnTopThreeSongs() throws Exception {
        // given
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<EnrichedSongEvent> input = newArrayList(
                aSongEvent()
                        .setUserId(1)
                        .setSong(aSong().name("Song 3").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:00:00.0Z").toEpochMilli())
                        .build(),
                aSongEvent()
                        .setUserId(2)
                        .setSong(aSong().name("Song 2").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:01:00.0Z").toEpochMilli())
                        .build(),
                aSongEvent()
                        .setUserId(3)
                        .setSong(aSong().name("Song 1").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:02:00.0Z").toEpochMilli())
                        .build(),
                aSongEvent()
                        .setUserId(4)
                        .setSong(aSong().name("Song 3").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:03:00.0Z").toEpochMilli())
                        .build(),
                aSongEvent()
                        .setUserId(5)
                        .setSong(aSong().name("Song 2").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:04:00.0Z").toEpochMilli())
                        .build(),
                aSongEvent()
                        .setUserId(6)
                        .setSong(aSong().name("Song 2").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:05:00.0Z").toEpochMilli())
                        .build()

        );

        DataStreamSource<EnrichedSongEvent> inputEvents = env.fromCollection(input);
        DataStream<SongsRanking> rankings = TopSongsRanking.pipeline(inputEvents);
        rankings.addSink(new CollectSink());

        // when
        env.execute();

        // then
        // there is one ranking generated
        assertEquals(CollectSink.values.size(), 1);

        // and the ranking contains songs in proper order
        SongsRanking ranking = CollectSink.values.get(0);
        assertEquals("Song 2", ranking.getTopSongs().get(0).getSong().getName());
        assertEquals(3L, ranking.getTopSongs().get(0).getCount());
        assertEquals("Song 3", ranking.getTopSongs().get(1).getSong().getName());
        assertEquals(2L, ranking.getTopSongs().get(1).getCount());
        assertEquals("Song 1", ranking.getTopSongs().get(2).getSong().getName());
        assertEquals(1L, ranking.getTopSongs().get(2).getCount());
    }

    /**
     * The static variable in CollectSink is used here because Flink serializes all operators before distributing them
     * across a cluster. Communicating with operators instantiated by a local Flink mini cluster via static variables
     * is one way around this issue. Alternatively, you could write the data to files in a temporary directory with
     * your test sink.
     */
    private static class CollectSink implements SinkFunction<SongsRanking> {

        // must be static
        static final List<SongsRanking> values = new ArrayList<>();

        @Override
        public synchronized void invoke(SongsRanking value, Context context) {
            values.add(value);
        }
    }

}
