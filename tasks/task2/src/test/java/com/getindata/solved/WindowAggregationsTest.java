package com.getindata.solved;

import com.getindata.tutorial.base.model.EnrichedSongEvent;
import com.getindata.tutorial.base.model.UserStatistics;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowAggregationsTest {

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
    void shouldAggregateUserStatistics() throws Exception {
        // given
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<EnrichedSongEvent> input = newArrayList(
                aSongEvent()
                        .setUserId(1)
                        .setSong(aSong().name("Song 1").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:00:00.0Z").toEpochMilli())
                        .build(),
                aSongEvent()
                        .setUserId(1)
                        .setSong(aSong().name("Song 2").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:03:00.0Z").toEpochMilli())
                        .build(),
                // gap between these two events is longer than allowed gap
                aSongEvent()
                        .setUserId(1)
                        .setSong(aSong().name("Song 3").build())
                        .setTimestamp(Instant.parse("2012-02-10T13:00:00.0Z").toEpochMilli())
                        .build(),
                aSongEvent()
                        .setUserId(1)
                        .setSong(aSong().name("Song 4").build())
                        .setTimestamp(Instant.parse("2012-02-10T13:02:00.0Z").toEpochMilli())
                        .build()

        );

        DataStreamSource<EnrichedSongEvent> inputEvents = env.fromCollection(input);
        DataStream<UserStatistics> statistics = WindowAggregations.pipeline(inputEvents);
        statistics.addSink(new CollectSink());

        // when
        env.execute();

        // then
        assertEquals(CollectSink.values.size(), 2);
        assertTrue(CollectSink.values.contains(
                UserStatistics.builder()
                        .userId(1)
                        .count(2)
                        // Session start == the time of the first event from the session.
                        .start(Instant.parse("2012-02-10T12:00:00.0Z").toEpochMilli())
                        // Session end == the time of the last event within the session + session gap.
                        .end(Instant.parse("2012-02-10T12:23:00.0Z").toEpochMilli())
                        .build()
        ));
        assertTrue(CollectSink.values.contains(
                UserStatistics.builder()
                        .userId(1)
                        .count(2)
                        // Session start == the time of the first event from the session.
                        .start(Instant.parse("2012-02-10T13:00:00.0Z").toEpochMilli())
                        // Session end == the time of the last event within the session + session gap.
                        .end(Instant.parse("2012-02-10T13:22:00.0Z").toEpochMilli())
                        .build()
        ));
    }

    @Test
    void shouldAggregateUserStatisticsForMultipleUsers() throws Exception {
        // given
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        final List<EnrichedSongEvent> input = newArrayList(
                aSongEvent()
                        .setUserId(2)
                        .setSong(aSong().name("Song 1").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:05:00.0Z").toEpochMilli())
                        .build(),
                aSongEvent()   // odd users are 5-minute delayed
                        .setUserId(1)
                        .setSong(aSong().name("Song 2").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:04:00.0Z").toEpochMilli())
                        .build(),
                aSongEvent()
                        .setUserId(2)
                        .setSong(aSong().name("Song 3").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:25:00.0Z").toEpochMilli())
                        .build(),
                aSongEvent()  // odd users are 5-minute delayed
                        .setUserId(1)
                        .setSong(aSong().name("Song 4").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:21:00.0Z").toEpochMilli())
                        .build()
        );

        DataStream<EnrichedSongEvent> inputEvents = env.fromCollection(input).setParallelism(1);
        DataStream<UserStatistics> statistics = WindowAggregations.pipeline(inputEvents);
        statistics.addSink(new CollectSink());

        // when
        env.execute();

        // then
        assertEquals(CollectSink.values.size(), 2);
        assertTrue(CollectSink.values.contains(
                UserStatistics.builder()
                        .userId(2)
                        .count(2)
                        // Session start == the time of the first event from the session.
                        .start(Instant.parse("2012-02-10T12:05:00.0Z").toEpochMilli())
                        // Session end == the time of the last event within the session + session gap.
                        .end(Instant.parse("2012-02-10T12:45:00.0Z").toEpochMilli())
                        .build()
        ));
        assertTrue(CollectSink.values.contains(
                UserStatistics.builder()
                        .userId(1)
                        .count(2)
                        // Session start == the time of the first event from the session.
                        .start(Instant.parse("2012-02-10T12:04:00.0Z").toEpochMilli())
                        // Session end == the time of the last event within the session + session gap.
                        .end(Instant.parse("2012-02-10T12:41:00.0Z").toEpochMilli())
                        .build()
        ));
    }

    /**
     * The static variable in CollectSink is used here because Flink serializes all operators before distributing them
     * across a cluster. Communicating with operators instantiated by a local Flink mini cluster via static variables
     * is one way around this issue. Alternatively, you could write the data to files in a temporary directory with
     * your test sink.
     */
    private static class CollectSink implements SinkFunction<UserStatistics> {

        // must be static
        static final List<UserStatistics> values = new ArrayList<>();

        @Override
        public synchronized void invoke(UserStatistics value, Context context) {
            values.add(value);
        }
    }

}
