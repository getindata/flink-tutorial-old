package com.getindata;

import com.getindata.tutorial.base.input.EnrichedSongsSource;
import com.getindata.tutorial.base.model.EnrichedSongEvent;
import com.getindata.tutorial.base.model.UserStatistics;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
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

    @VisibleForTesting
    static DataStream<UserStatistics> pipeline(DataStream<EnrichedSongEvent> source) {
        return source
                .assignTimestampsAndWatermarks(new SongWatermarkStrategy())
                .keyBy(new SongKeySelector())
                .<TimeWindow>window(null /* TODO fill in the code */)
                .aggregate(
                        new SongAggregationFunction(),
                        new SongWindowFunction()
                );
    }

    static class SongWatermarkStrategy implements WatermarkStrategy<EnrichedSongEvent> {
        @Override
        public WatermarkGenerator<EnrichedSongEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<>() {
                @Override
                public void onEvent(EnrichedSongEvent event, long eventTimestamp, WatermarkOutput output) {
                    /* TODO fill in the code */
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    // don't need to do anything because we emit in reaction to events above
                }
            };
        }

        @Override
        public TimestampAssigner<EnrichedSongEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (element, recordTimestamp) -> 0L;    // TODO: return proper value
        }
    }

    static class SongKeySelector implements KeySelector<EnrichedSongEvent, Integer> {
        @Override
        public Integer getKey(EnrichedSongEvent songEvent) throws Exception {
            //TODO fill in the code
            return null;
        }
    }

    static class SongAggregationFunction implements AggregateFunction<EnrichedSongEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(EnrichedSongEvent songEvent, Long count) {
            //TODO fill in the code
            return count;
        }

        @Override
        public Long getResult(Long count) {
            //TODO fill in the code
            return 0L;

        }

        @Override
        public Long merge(Long count1, Long count2) {
            //TODO fill in the code
            return 0L;
        }
    }

    static class SongWindowFunction implements WindowFunction<Long, UserStatistics, Integer, TimeWindow> {
        @Override
        public void apply(Integer userId, TimeWindow window, Iterable<Long> input, Collector<UserStatistics> out) {
            // TODO fill in the code
            // HINT: to create UserStatistics, use UserStatistics.builder()(...)
        }
    }
}
