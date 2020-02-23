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

import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.SongCount;
import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.utils.shortcuts.Shortcuts;
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

import java.time.Duration;
import java.time.Instant;


public class AdvancedTimeHandling {

    public static void main(String[] args) throws Exception {
        final String userName = KafkaProperties.getUsername();
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<SongEvent, Integer> keyedSongs = Shortcuts.getSongsWithTimestamps(sEnv, userName)
                .filter(new TheRollingStonesFilterFunction())
                .keyBy(new UserKeySelector());

        DataStream<SongCount> counts = keyedSongs.process(new SongCountingProcessFunction());

        counts.print();

        sEnv.execute();
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
                LOG.info("A user {} listens to The Rolling Stones song for the first time.", context.getCurrentKey());
                // Initialize state.
                counterState.update(1);
                lastTimestampState.update(songEvent.getTimestamp());
            } else {
                LOG.info("A user {} listens to a next ({}) The Rolling Stones song.", context.getCurrentKey(), currentCounter + 1);
                counterState.update(currentCounter + 1);
                lastTimestamp = Math.max(lastTimestamp, songEvent.getTimestamp());
                lastTimestampState.update(lastTimestamp);
                context.timerService().registerEventTimeTimer(lastTimestamp + FIFTEEN_MINUTES);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SongCount> out) throws Exception {
            Integer currentCounter = counterState.value();
            Long lastTimestamp = lastTimestampState.value();

            // if now() >= lastTimestamp + 15 minutes
            if (!Instant.ofEpochMilli(ctx.timestamp()).isBefore(Instant.ofEpochMilli(lastTimestamp).plus(Duration.ofMinutes(15)))) {
                LOG.info("Fifteen minutes has passed; userId={}.", ctx.getCurrentKey());
                if (currentCounter >= 3) {
                    LOG.info("Emitting counter={} for userId={}.", currentCounter, ctx.getCurrentKey());
                    out.collect(new SongCount(ctx.getCurrentKey(), currentCounter));
                }
                counterState.clear();
                lastTimestampState.clear();
            }
        }
    }
}
