package com.getindata;

import com.getindata.tutorial.base.model.SongEventAvro;
import com.getindata.tutorial.base.model.UserStatisticsAvro;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
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

        final KafkaSource<SongEventAvro> source = KafkaSource.<SongEventAvro>builder()
                // TODO fill in the code
                .build();


        final KafkaSink<UserStatisticsAvro> sink = KafkaSink.<UserStatisticsAvro>builder()
                // TODO fill in the code
                .build();

        final DataStream<SongEventAvro> events = sEnv.fromSource(source, new SongWatermarkStrategy(), "Kafka Source");
        final DataStream<UserStatisticsAvro> statistics = pipeline(events);
        statistics.sinkTo(sink);

        // execute streams
        sEnv.execute();
    }

    @VisibleForTesting
    static DataStream<UserStatisticsAvro> pipeline(DataStream<SongEventAvro> source) {
        // song plays in user sessions
        final WindowedStream<SongEventAvro, Integer, TimeWindow> windowedStream = source
                .keyBy(new UserIdSelector())
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
            return new WatermarkGenerator<>() {
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

    static class UserIdSelector implements KeySelector<SongEventAvro, Integer> {
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
