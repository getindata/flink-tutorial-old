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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.model.SongEventType;
import com.getindata.tutorial.base.utils.Alert;
import com.getindata.tutorial.base.utils.shortcuts.Shortcuts;
import org.joda.time.Duration;

import java.util.List;
import java.util.Map;

public class AlertsWithCep {

	public static final String SONG_PLAYED = "song played";
	public static final String SONG_PAUSED = "song paused";

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// You can use prepared code for reading events from kafka
		final DataStream<SongEvent> songsInEventTime = Shortcuts.getSongsWithTimestamps(sEnv, "lion")
				.keyBy(new KeySelector<SongEvent, Integer>() {
					@Override
					public Integer getKey(SongEvent songEvent) throws Exception {
						return songEvent.getUserId();
					}
				});

		// Create appropriate pattern
		final Pattern<SongEvent, SongEvent> pattern = Pattern.<SongEvent>begin(SONG_PLAYED).where(new SimpleCondition<SongEvent>() {
			@Override
			public boolean filter(SongEvent songEvent) throws Exception {
				return songEvent.getType() == SongEventType.PLAY;
			}
		}).followedBy(SONG_PAUSED).where(new IterativeCondition<SongEvent>() {
			@Override
			public boolean filter(SongEvent songEvent, Context<SongEvent> context) throws Exception {

				if (songEvent.getType() != SongEventType.PAUSE) {
					return false;
				}

				final SongEvent songPlayedEvent = context.getEventsForPattern(SONG_PLAYED).iterator().next();
				return isSameName(songEvent, songPlayedEvent) &&
				       isShortEnough(songEvent, songPlayedEvent);
			}
		});

		// Apply pattern to stream
		final PatternStream<SongEvent> matchStream = CEP.pattern(songsInEventTime, pattern);

		// Convert match into Alert
		matchStream.select(new PatternSelectFunction<SongEvent, Alert>() {
			@Override
			public Alert select(Map<String, List<SongEvent>> map) throws Exception {
				final SongEvent start = map.get(SONG_PLAYED).get(0);
				final SongEvent end = map.get(SONG_PAUSED).get(0);

				return new Alert(
						start.getSong().getName(),
						start.getTimestamp(),
						end.getTimestamp(),
						start.getUserId());
			}
		}).print();

		sEnv.execute();
	}

	private static boolean isSameName(SongEvent songEvent, SongEvent songPlayedEvent) {
		return songPlayedEvent.getSong().getName().equals(songEvent.getSong().getName());
	}

	private static boolean isShortEnough(SongEvent songEvent, SongEvent songPlayedEvent) {
		return Duration.millis(songEvent.getTimestamp() - songPlayedEvent.getTimestamp())
				       .compareTo(Duration.standardSeconds(15)) < 0;
	}
}
