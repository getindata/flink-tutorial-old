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

package com.getindata.tutorial.solutions.advanced;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.utils.UserStatistics;
import com.getindata.tutorial.base.utils.shortcuts.Shortcuts;

import java.time.Instant;

public class AdvancedTimeHandling {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// You can use prepared code for reading events from kafka
		final DataStream<SongEvent> songsInEventTime = Shortcuts.getSongsWithTimestamps(sEnv)
				.filter(ev -> ev.getUserId() == 1)
				.keyBy(SongEvent::getUserId);

		songsInEventTime.process(new ProcessFunction<SongEvent, UserStatistics>() {

			/** The state that is maintained by this process function */
			private ValueState<UserStatistics> state;

			@Override
			public void open(Configuration parameters) throws Exception {
				state = getRuntimeContext().getState(new ValueStateDescriptor<>(
						"userStatistics",
						UserStatistics.class));
			}

			@Override
			public void processElement(
					SongEvent songEvent,
					Context context,
					Collector<UserStatistics> collector) throws Exception {

				UserStatistics current = state.value();
				if (current == null) {
					final Instant startOfWindow = Instant.ofEpochMilli(songEvent.getTimestamp());
					final Instant endOfWindow = Instant.ofEpochMilli(songEvent.getTimestamp()).plusSeconds(15);

					current = new UserStatistics(
							songEvent.getUserId(),
							1,
							startOfWindow,
							endOfWindow);

					state.update(current);
					context.timerService().registerEventTimeTimer(startOfWindow.plusSeconds(5).toEpochMilli());
					context.timerService().registerEventTimeTimer(endOfWindow.toEpochMilli());
				} else {
					current.setCount(current.getCount() + 1);
					state.update(current);
				}

			}

			@Override
			public void onTimer(long timestamp, OnTimerContext ctx, Collector<UserStatistics> out) throws Exception {
				UserStatistics current = state.value();
				if (current == null) {
					throw new IllegalStateException("This should not happen!");
				} else {
					if (current.getEnd().isAfter(Instant.ofEpochMilli(timestamp).plusSeconds(5))) {
						ctx.timerService().registerEventTimeTimer(Instant.ofEpochMilli(timestamp)
								.plusSeconds(5)
								.toEpochMilli());
					} else if (current.getEnd().equals(Instant.ofEpochMilli(timestamp))) {
						state.clear();
					}
					out.collect(current);
				}
			}
		}).print();

		sEnv.execute();
	}
}
