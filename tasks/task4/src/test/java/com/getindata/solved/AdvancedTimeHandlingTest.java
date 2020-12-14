package com.getindata.solved;

import com.getindata.solved.AdvancedTimeHandling.SongCountingProcessFunction;
import com.getindata.solved.AdvancedTimeHandling.UserKeySelector;
import com.getindata.tutorial.base.model.EnrichedSongEvent;
import com.getindata.tutorial.base.model.SongCount;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import static com.getindata.tutorial.base.model.TestDataBuilders.aRollingStonesSongEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AdvancedTimeHandlingTest {

    @Test
    void shouldEmitNotificationIfUserListensToTheBandAtLeastThreeTimes() throws Exception {
        SongCountingProcessFunction function = new SongCountingProcessFunction();
        KeyedOneInputStreamOperatorTestHarness<Integer, EnrichedSongEvent, SongCount> harness = getHarness(function);

        harness.open();

        harness.processElement(
                aRollingStonesSongEvent().setUserId(10).setTimestamp(Instant.parse("2020-02-10T12:00:00.0Z").toEpochMilli()).build(),
                Instant.parse("2020-02-10T12:00:00.0Z").toEpochMilli()
        );
        harness.processElement(
                aRollingStonesSongEvent().setUserId(10).setTimestamp(Instant.parse("2020-02-10T12:05:00.0Z").toEpochMilli()).build(),
                Instant.parse("2020-02-10T12:05:00.0Z").toEpochMilli()
        );
        harness.processElement(
                aRollingStonesSongEvent().setUserId(10).setTimestamp(Instant.parse("2020-02-10T12:10:00.0Z").toEpochMilli()).build(),
                Instant.parse("2020-02-10T12:10:00.0Z").toEpochMilli()
        );
        harness.processWatermark(Instant.parse("2020-02-10T12:25:00.0Z").toEpochMilli());

        List<SongCount> output = getResults(harness);

        assertEquals(1, output.size());
        assertEquals(new SongCount(10, 3), output.get(0));
    }

    @Test
    void shouldNotEmitNotificationIfGapBetweenSongsIsTooLong() throws Exception {
        SongCountingProcessFunction function = new SongCountingProcessFunction();
        KeyedOneInputStreamOperatorTestHarness<Integer, EnrichedSongEvent, SongCount> harness = getHarness(function);

        harness.open();

        harness.processElement(
                aRollingStonesSongEvent().setUserId(10).setTimestamp(Instant.parse("2020-02-10T12:00:00.0Z").toEpochMilli()).build(),
                Instant.parse("2020-02-10T12:00:00.0Z").toEpochMilli()
        );
        harness.processElement(
                aRollingStonesSongEvent().setUserId(10).setTimestamp(Instant.parse("2020-02-10T12:05:00.0Z").toEpochMilli()).build(),
                Instant.parse("2020-02-10T12:05:00.0Z").toEpochMilli()
        );
        // gap is longer than 15 minutes
        harness.processWatermark(Instant.parse("2020-02-10T12:25:00.0Z").toEpochMilli());
        harness.processElement(
                aRollingStonesSongEvent().setUserId(10).setTimestamp(Instant.parse("2020-02-10T12:30:00.0Z").toEpochMilli()).build(),
                Instant.parse("2020-02-10T12:30:00.0Z").toEpochMilli()
        );
        harness.processWatermark(Instant.parse("2020-02-10T12:25:00.0Z").toEpochMilli());

        List<SongCount> output = getResults(harness);

        assertEquals(0, output.size());
    }


    private KeyedOneInputStreamOperatorTestHarness<Integer, EnrichedSongEvent, SongCount> getHarness(SongCountingProcessFunction function) throws Exception {
        KeyedProcessOperator<Integer, EnrichedSongEvent, SongCount> keyedProcessOperator = new KeyedProcessOperator<>(function);
        return new KeyedOneInputStreamOperatorTestHarness<>(
                keyedProcessOperator,
                new UserKeySelector(),
                TypeInformation.of(Integer.class)
        );
    }

    private List<SongCount> getResults(KeyedOneInputStreamOperatorTestHarness<Integer, EnrichedSongEvent, SongCount> harness) {
        return harness.extractOutputStreamRecords()
                .stream()
                .map(StreamRecord::getValue)
                .collect(Collectors.toList());
    }

}