package com.getindata.solved;

import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.solved.SongEventAvro;
import com.getindata.tutorial.base.model.solved.UserStatisticsAvro;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class KafkaWindowAggregations {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        final KafkaSource<SongEventAvro> source = KafkaSource.<SongEventAvro>builder()
                .setBootstrapServers(KafkaProperties.BOOTSTRAP_SERVERS)
                .setTopics(KafkaProperties.INPUT_AVRO_TOPIC)
                .setGroupId(KafkaProperties.GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                SongEventAvro.class,
                                KafkaProperties.SCHEMA_REGISTRY_URL)
                )
                .build();

        final KafkaSink<UserStatisticsAvro> sink = KafkaSink.<UserStatisticsAvro>builder()
                .setBootstrapServers(KafkaProperties.BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<UserStatisticsAvro>builder()
                                .setTopic(KafkaProperties.OUTPUT_AVRO_TOPIC)
                                .setValueSerializationSchema(
                                        ConfluentRegistryAvroSerializationSchema.forSpecific(
                                                UserStatisticsAvro.class,
                                                UserStatisticsAvro.class.getSimpleName(),
                                                KafkaProperties.SCHEMA_REGISTRY_URL)
                                )
                                .build()
                )
                .build();

        final DataStream<SongEventAvro> events = sEnv.fromSource(source, new SongWatermarkStrategy(), "Kafka Source");
        final DataStream<UserStatisticsAvro> statistics = pipeline(events);
        statistics.sinkTo(sink);

        // execute streams
        sEnv.execute();
    }

    @VisibleForTesting
    static DataStream<UserStatisticsAvro> pipeline(DataStream<SongEventAvro> source) {
        final DataStream<SongEventAvro> eventsInEventTime = source.assignTimestampsAndWatermarks(
                new SongWatermarkStrategy());

        // song plays in user sessions
        final WindowedStream<SongEventAvro, Integer, TimeWindow> windowedStream = eventsInEventTime
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
        public TimestampAssigner<SongEventAvro> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (element, recordTimestamp) -> element.getTimestamp();
        }
    }

    static class UserIdSelector implements KeySelector<SongEventAvro, Integer> {
        @Override
        public Integer getKey(SongEventAvro songEvent) {
            return songEvent.getUserId();
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
                            .setUserId(userId)
                            .setCount(sum)
                            .setStart(Instant.ofEpochMilli(window.getStart()))
                            .setEnd(Instant.ofEpochMilli(window.getEnd()))
                            .setDuration(window.getEnd() - window.getStart())
                            .build()
            );
        }
    }
}
