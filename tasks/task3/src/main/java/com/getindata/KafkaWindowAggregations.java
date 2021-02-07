package com.getindata;

import com.getindata.tutorial.base.model.SongEventAvro;
import com.getindata.tutorial.base.model.UserStatisticsAvro;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class KafkaWindowAggregations {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a stream of events from source
        final DataStream<SongEventAvro> events = sEnv.addSource(
                /* TODO put your code here */
                null
        );

        final DataStream<UserStatisticsAvro> statistics = pipeline(events);

        statistics.addSink(
                /* TODO put your code here */
                null
        );

        // execute streams
        sEnv.execute();
    }


    static DataStream<UserStatisticsAvro> pipeline(DataStream<SongEventAvro> source) {
        final DataStream<SongEventAvro> eventsInEventTime = source.assignTimestampsAndWatermarks(new SongWatermarkStrategy());

        // song plays in user sessions
        final WindowedStream<SongEventAvro, Integer, TimeWindow> windowedStream = eventsInEventTime
                .filter(new SongFilterFunction())
                .keyBy(new SongKeySelector())
                .window(EventTimeSessionWindows.withGap(Time.minutes(20)));

        return windowedStream.aggregate(
                new SongAggregationFunction(),
                new SongWindowFunction()
        );
    }

    static class SongWatermarkStrategy implements WatermarkStrategy<SongEventAvro> {

        private static final long FIVE_MINUTES = 5 * 1000 * 60L;

        @Override
        public WatermarkGenerator<SongEventAvro> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<SongEventAvro>() {
                @Override
                public void onEvent(SongEventAvro songEvent, long eventTimestamp, WatermarkOutput output) {
                    /* TODO put your code here */
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    // don't need to do anything because we emit in reaction to events above
                }
            };
        }

        @Override
        public TimestampAssigner<SongEventAvro> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            /* TODO put your code here */
            return null;
        }
    }

    static class SongFilterFunction implements FilterFunction<SongEventAvro> {
        @Override
        public boolean filter(final SongEventAvro songEvent) {
            /* TODO put your code here */
            return false;
        }
    }

    static class SongKeySelector implements KeySelector<SongEventAvro, Integer> {
        @Override
        public Integer getKey(SongEventAvro songEvent) {
            /* TODO put your code here */
            return null;
        }
    }

    static class SongAggregationFunction implements AggregateFunction<SongEventAvro, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(SongEventAvro songEvent, Long count) {
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

    static class SongWindowFunction implements WindowFunction<Long, UserStatisticsAvro, Integer, TimeWindow> {
        @Override
        public void apply(Integer userId, TimeWindow window, Iterable<Long> input, Collector<UserStatisticsAvro> out) {
            long sum = 0;
            for (Long l : input) {
                sum += l;
            }

            out.collect(
                    UserStatisticsAvro.newBuilder()
                            /* TODO put your code here */
                            .build()
            );
        }
    }
}
