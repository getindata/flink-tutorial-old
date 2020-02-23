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

package com.getindata;

import com.getindata.tutorial.base.input.SongsSource;
import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.model.UserStatistics;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class WindowAggregations {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO set time characteristics

        // create a stream of events from source
        final DataStream<SongEvent> events = sEnv.addSource(new SongsSource());
        final DataStream<SongEvent> eventsInEventTime = events.assignTimestampsAndWatermarks(new SongWatermarkAssigner());

        // song plays in user sessions
        final WindowedStream<SongEvent, Integer, TimeWindow> windowedStream = eventsInEventTime
                .filter(new SongFilterFunction())
                .keyBy(new SongKeySelector())
                .window(null /* TODO fill in the code */);

        final DataStream<UserStatistics> statistics = windowedStream.aggregate(
                new SongAggregationFunction(),
                new SongWindowFunction()
        );

        statistics.print();

        // execute streams
        sEnv.execute();
    }

    static class SongWatermarkAssigner implements AssignerWithPunctuatedWatermarks<SongEvent> {
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(SongEvent songEvent, long lastTimestamp) {
            //TODO fill in the code

            // HINT: to access event timestamp, use songEvent.getTimestamp()
            // HINT: To access user id, use songEvent.getUserId()
            return null;
        }

        @Override
        public long extractTimestamp(SongEvent songEvent, long lastTimestamp) {
            //TODO fill in the code
            return 0;

        }
    }

    static class SongFilterFunction implements FilterFunction<SongEvent> {
        @Override
        public boolean filter(final SongEvent songEvent) {
            //TODO fill in the code
            return true;
        }
    }

    static class SongKeySelector implements KeySelector<SongEvent, Integer> {
        @Override
        public Integer getKey(SongEvent songEvent) throws Exception {
            //TODO fill in the code
            return null;
        }
    }

    static class SongAggregationFunction implements AggregateFunction<SongEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(SongEvent songEvent, Long count) {
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
