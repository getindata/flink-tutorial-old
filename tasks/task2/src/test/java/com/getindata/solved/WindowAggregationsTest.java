package com.getindata.solved;

import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.model.SongEventType;
import com.getindata.tutorial.base.model.UserStatistics;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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
    void shouldAggregateUserStatisticsForMultipleUsers() throws Exception {
        // given
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final List<SongEvent> input = newArrayList(
                aSongEvent()
                        .setUserId(2)
                        .setSong(aSong().name("Song 1").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:06:00.0Z").toEpochMilli())
                        .setType(SongEventType.PLAY)
                        .build(),
                aSongEvent()   // odd users are 5-minute delayed
                        .setUserId(1)
                        .setSong(aSong().name("Song 2").build())
                        .setTimestamp(Instant.parse("2012-02-10T12:00:00.0Z").toEpochMilli())
                        .setType(SongEventType.PLAY)
                        .build()
        );

        DataStream<SongEvent> inputEvents = env.fromCollection(input);
        DataStream<UserStatistics> statistics = WindowAggregations.pipeline(inputEvents);
        statistics.addSink(new CollectSink());

        // when
        env.execute();

        // then
        assertEquals(CollectSink.values.size(), 1);

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