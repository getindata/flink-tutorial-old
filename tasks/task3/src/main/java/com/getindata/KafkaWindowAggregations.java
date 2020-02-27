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

import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.model.SongEventType;
import com.getindata.tutorial.base.model.UserStatistics;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class KafkaWindowAggregations {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create a stream of events from source
        final DataStream<SongEvent> events = sEnv.addSource(
                /* TODO put your code here */
                null
        );

        final DataStream<UserStatistics> statistics = pipeline(events);

        statistics.addSink(
                /* TODO put your code here */
                null
        );

        // execute streams
        sEnv.execute();
    }


    static DataStream<UserStatistics> pipeline(DataStream<SongEvent> source) {
        final DataStream<SongEvent> eventsInEventTime = source.assignTimestampsAndWatermarks(new SongWatermarkAssigner());

        // song plays in user sessions
        final WindowedStream<SongEvent, Integer, TimeWindow> windowedStream = eventsInEventTime
                .filter(new SongFilterFunction())
                .keyBy(new SongKeySelector())
                .window(EventTimeSessionWindows.withGap(Time.minutes(20)));

        return windowedStream.aggregate(
                new SongAggregationFunction(),
                new SongWindowFunction()
        );
    }

    static class SongWatermarkAssigner implements AssignerWithPunctuatedWatermarks<SongEvent> {

        private static final long FIVE_MINUTES = 5 * 1000 * 60L;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(SongEvent songEvent, long lastTimestamp) {
            return songEvent.getUserId() % 2 == 1
                    ? new Watermark(songEvent.getTimestamp())
                    : new Watermark(songEvent.getTimestamp() - FIVE_MINUTES);

        }

        @Override
        public long extractTimestamp(SongEvent songEvent, long lastTimestamp) {
            return songEvent.getTimestamp();
        }
    }

    static class SongFilterFunction implements FilterFunction<SongEvent> {
        @Override
        public boolean filter(final SongEvent songEvent) {
            return songEvent.getType() == SongEventType.PLAY;
        }
    }

    static class SongKeySelector implements KeySelector<SongEvent, Integer> {
        @Override
        public Integer getKey(SongEvent songEvent) {
            return songEvent.getUserId();
        }
    }

    static class SongAggregationFunction implements AggregateFunction<SongEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(SongEvent songEvent, Long count) {
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
