package com.getindata;

import com.getindata.tutorial.base.input.EnrichedSongsSource;
import com.getindata.tutorial.base.model.EnrichedSongEvent;
import com.getindata.tutorial.base.model.SongCount;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AdvancedTimeHandling {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<EnrichedSongEvent, Integer> keyedSongs = sEnv
                .addSource(new EnrichedSongsSource())
                .assignTimestampsAndWatermarks(new SongWatermarkStrategy())
                .filter(new TheRollingStonesFilterFunction())
                .keyBy(new UserKeySelector());

        DataStream<SongCount> counts = keyedSongs.process(new SongCountingProcessFunction());

        counts.print();

        sEnv.execute();
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

    static class TheRollingStonesFilterFunction implements FilterFunction<EnrichedSongEvent> {
        @Override
        public boolean filter(EnrichedSongEvent songEvent) {
            return songEvent.getSong().getAuthor().equals("The Rolling Stones");
        }
    }

    static class UserKeySelector implements KeySelector<EnrichedSongEvent, Integer> {
        @Override
        public Integer getKey(EnrichedSongEvent songEvent) {
            return songEvent.getUserId();
        }
    }

    static class SongCountingProcessFunction extends KeyedProcessFunction<Integer, EnrichedSongEvent, SongCount> {

        private static final Logger LOG = LoggerFactory.getLogger(SongCountingProcessFunction.class);

        private static final long FIFTEEN_MINUTES = 15 * 60 * 1000L;
        private static final long THRESHOLD = 3;

        /**
         * The state that is maintained by this process function
         */
        private ValueState<Integer> counterState;
        private ValueState<Long> lastTimestampState;

        @Override
        public void open(Configuration parameters) {
            counterState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                    "counter",
                    Integer.class
            ));
            lastTimestampState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                    "lastTimestamp",
                    Long.class
            ));
        }

        @Override
        public void processElement(EnrichedSongEvent songEvent, Context context, Collector<SongCount> collector) throws Exception {
            Integer currentCounter = counterState.value();
            Long lastTimestamp = lastTimestampState.value();

            if (currentCounter == null) {
                // Initialize state.
                // TODO put your code here
            } else {
                // Update state.
                // TODO put your code here
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SongCount> out) throws Exception {
            Long lastTimestamp = lastTimestampState.value();

            // TODO put your code here
        }
    }
}
