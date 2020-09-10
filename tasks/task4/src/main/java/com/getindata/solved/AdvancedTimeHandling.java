/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.getindata.solved;

import com.getindata.tutorial.base.input.SongsSource;
import com.getindata.tutorial.base.model.SongCount;
import com.getindata.tutorial.base.model.SongEvent;
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

        KeyedStream<SongEvent, Integer> keyedSongs =
                sEnv.addSource(new SongsSource())
                        .assignTimestampsAndWatermarks(new SongWatermarkStrategy())
                        .filter(new TheRollingStonesFilterFunction())
                        .keyBy(new UserKeySelector());

        DataStream<SongCount> counts = keyedSongs.process(new SongCountingProcessFunction());

        counts.print();

        sEnv.execute();
    }

    static class SongWatermarkStrategy implements WatermarkStrategy<SongEvent> {

        private static final long FIVE_MINUTES = 5 * 1000 * 60L;

        @Override
        public WatermarkGenerator<SongEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<SongEvent>() {
                @Override
                public void onEvent(SongEvent songEvent, long eventTimestamp, WatermarkOutput output) {
                    org.apache.flink.api.common.eventtime.Watermark watermark = songEvent.getUserId() % 2 == 1
                            ? new org.apache.flink.api.common.eventtime.Watermark(songEvent.getTimestamp())
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
        public TimestampAssigner<SongEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (element, recordTimestamp) -> element.getTimestamp();
        }
    }

    static class TheRollingStonesFilterFunction implements FilterFunction<SongEvent> {
        @Override
        public boolean filter(SongEvent songEvent) {
            return songEvent.getSong().getAuthor().equals("The Rolling Stones");
        }
    }

    static class UserKeySelector implements KeySelector<SongEvent, Integer> {
        @Override
        public Integer getKey(SongEvent songEvent) {
            return songEvent.getUserId();
        }
    }

    static class SongCountingProcessFunction extends KeyedProcessFunction<Integer, SongEvent, SongCount> {

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
        public void processElement(SongEvent songEvent, Context context, Collector<SongCount> collector) throws Exception {
            Integer currentCounter = counterState.value();
            Long lastTimestamp = lastTimestampState.value();
            if (currentCounter == null) {
                LOG.debug("A user {} listens to The Rolling Stones song for the first time.", context.getCurrentKey());
                // Initialize state.
                counterState.update(1);
                lastTimestampState.update(context.timestamp());
            } else {
                currentCounter++;
                LOG.debug("A user {} listens to a next ({}) The Rolling Stones song.", context.getCurrentKey(), currentCounter);
                if (currentCounter >= THRESHOLD) {
                    collector.collect(new SongCount(context.getCurrentKey(), currentCounter));
                }
                counterState.update(currentCounter);
                lastTimestamp = Math.max(lastTimestamp, context.timestamp());
                lastTimestampState.update(lastTimestamp);
                context.timerService().registerEventTimeTimer(lastTimestamp + FIFTEEN_MINUTES);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SongCount> out) throws Exception {
            Long lastTimestamp = lastTimestampState.value();

            if (ctx.timestamp() >= lastTimestamp + FIFTEEN_MINUTES) {
                LOG.debug("Fifteen minutes has passed for userId={}. Clearing state.", ctx.getCurrentKey());
                counterState.clear();
                lastTimestampState.clear();
            }
        }
    }
}
