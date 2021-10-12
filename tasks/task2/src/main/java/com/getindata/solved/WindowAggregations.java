package com.getindata.solved;

import com.getindata.tutorial.base.input.EnrichedSongsSource;
import com.getindata.tutorial.base.model.EnrichedSongEvent;
import com.getindata.tutorial.base.model.UserStatistics;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowAggregations {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a stream of events from source
        final DataStream<EnrichedSongEvent> events = sEnv.addSource(new EnrichedSongsSource());
        // In order not to copy the whole pipeline code from production to test, we made sources and sinks pluggable in
        // the production code so that we can now inject test sources and test sinks in the tests.
        final DataStream<UserStatistics> statistics = pipeline(events);

        // print results
        statistics.print();

        // execute streams
        sEnv.execute();
    }

    static DataStream<UserStatistics> pipeline(DataStream<EnrichedSongEvent> source) {
        return source
                .assignTimestampsAndWatermarks(new SongWatermarkStrategy())
                .keyBy(new UserKeySelector())
                .window(EventTimeSessionWindows.withGap(Time.minutes(20)))
                .aggregate(
                        new SongAggregationFunction(),
                        new SongWindowFunction()
                );
    }

    static class SongWatermarkStrategy implements WatermarkStrategy<EnrichedSongEvent> {

        private static final long FIVE_MINUTES = 5 * 1000 * 60L;

        @Override
        public WatermarkGenerator<EnrichedSongEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<EnrichedSongEvent>() {
                @Override
                public void onEvent(EnrichedSongEvent songEvent, long eventTimestamp, WatermarkOutput output) {
                    Watermark watermark = songEvent.getUserId() % 2 == 1
                            ? new Watermark(songEvent.getTimestamp())
                            : new Watermark(songEvent.getTimestamp() - FIVE_MINUTES);
                    output.emitWatermark(watermark);
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    // don't need to do anything because we emit in reaction to events above
                }
            };
        }

        @Override
        public TimestampAssigner<EnrichedSongEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (element, recordTimestamp) -> element.getTimestamp();
        }
    }

    static class UserKeySelector implements KeySelector<EnrichedSongEvent, Integer> {
        @Override
        public Integer getKey(EnrichedSongEvent songEvent) {
            return songEvent.getUserId();
        }
    }

    static class SongAggregationFunction implements AggregateFunction<EnrichedSongEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(EnrichedSongEvent songEvent, Long count) {
            return count + 1;
        }

        @Override
        public Long getResult(Long count) {
            return count;

        }

        @Override
        public Long merge(Long count1, Long count2) {
            return count1 + count2;
        }

    }

    static class SongWindowFunction implements WindowFunction<Long, UserStatistics, Integer, TimeWindow> {
        @Override
        public void apply(Integer userId, TimeWindow window, Iterable<Long> input, Collector<UserStatistics> out) {
            long sum = 0;
            for (Long l : input) {
                sum += l;
            }

            out.collect(
                    UserStatistics.builder()
                            .userId(userId)
                            .count(sum)
                            .start(window.getStart())
                            .end(window.getEnd())
                            .build()
            );
        }
    }
}
