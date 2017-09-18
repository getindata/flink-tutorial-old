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

package com.getindata.tutorial.solutions.basic;

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

import com.getindata.tutorial.base.input.SongsSource;
import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.model.SongEventType;
import com.getindata.tutorial.base.utils.CountAggregator;
import com.getindata.tutorial.base.utils.UserStatistics;

import javax.annotation.Nullable;

public class WindowAggregations {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// create a stream of events from source
		final DataStream<SongEvent> events = sEnv.addSource(new SongsSource());

		final DataStream<SongEvent> eventsInEventTime = events.assignTimestampsAndWatermarks(
				/* INSERT YOUR CODE HERE */
		);

		// song plays in user sessions
		final WindowedStream<SongEvent, Integer, TimeWindow> windowedStream = eventsInEventTime
				.filter(/* INSERT YOUR CODE HERE */)
				.keyBy(/* INSERT YOUR CODE HERE */)
				.window(/* INSERT YOUR CODE HERE */);

		final DataStream<UserStatistics> statistics = windowedStream.aggregate(
				new AggregateFunction<SongEvent, CountAggregator, Long>() {
					@Override
					public CountAggregator createAccumulator() {
						/* INSERT YOUR CODE HERE */
					}

					@Override
					public void add(
							SongEvent songEvent, CountAggregator countAggregator) {
						/* INSERT YOUR CODE HERE */
					}

					@Override
					public Long getResult(CountAggregator countAggregator) {
						/* INSERT YOUR CODE HERE */
					}

					@Override
					public CountAggregator merge(
							CountAggregator countAggregator, CountAggregator acc1) {
						/* INSERT YOUR CODE HERE */
					}
				}, new WindowFunction<Long, UserStatistics, Integer, TimeWindow>() {
					@Override
					public void apply(
							Integer userId,
							TimeWindow window,
							Iterable<Long> input,
							Collector<UserStatistics> out) throws Exception {

						/* INSERT YOUR CODE HERE */
					}
				});

		statistics.print();

		// execute streams
		sEnv.execute();
	}
}
