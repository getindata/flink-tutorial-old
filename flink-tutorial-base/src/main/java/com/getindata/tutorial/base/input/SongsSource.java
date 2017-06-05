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

package com.getindata.tutorial.base.input;

import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.getindata.tutorial.base.model.Song;
import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.model.SongEventType;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;

public class SongsSource extends RichParallelSourceFunction<SongEvent> {

	private static final List<Song> songs = Lists.newArrayList(
			new Song(toMillis(2, 40), "Yellow Submarine", "The Beatles"),
			new Song(toMillis(2, 59), "Get Off Of My Cloud", "The Rolling Stones"),
			new Song(toMillis(5, 28), "Let It Bleed", "The Rolling Stones"),
			new Song(toMillis(3, 51), "Dancing Queen", "Abba"),
			new Song(toMillis(3, 53), "Rolling in the Deep", "Adele"),
			new Song(toMillis(3, 11), "Killer Queen", "Queen"),
			new Song(toMillis(3, 54), "California Gurls", "Katy Perry"),
			new Song(toMillis(4, 57), "Silent All These Years", "Tori Amos"),
			new Song(toMillis(6, 6), "Bohemian Rhapsody", "Queen"),
			new Song(toMillis(4, 32), "I want to break free", "Queen")
	);

	private static long toMillis(int minutes, int seconds) {
		return Duration.ofMinutes(minutes).plus(seconds, ChronoUnit.SECONDS).toMillis();
	}

	private boolean isRunning = true;

	public static Time getMaximalLag() {
		return Time.milliseconds(songs.stream().mapToLong(Song::getLength).max().orElse(0));
	}

	@Override
	public void run(SourceContext<SongEvent> sourceContext) throws Exception {
		while (isRunning) {
			final Random random = new Random();

			int userId = random.nextInt(50);
			Song song = songs.get(random.nextInt(songs.size()));

			double caseExample = random.nextDouble();

			final long timestamp = System.currentTimeMillis();
			if (caseExample < 0.6) {
				sourceContext.collect(new SongEvent(song, timestamp, SongEventType.PLAY, userId));
			} else if (caseExample < 0.9) {
				sourceContext.collect(new SongEvent(song, timestamp, SongEventType.PLAY, userId));
				sourceContext.collect(new SongEvent(
						song,
						timestamp + random.nextInt((int) song.getLength()),
						SongEventType.SKIP,
						userId));
			} else {
				sourceContext.collect(new SongEvent(song, timestamp, SongEventType.PLAY, userId));
				sourceContext.collect(new SongEvent(
						song,
						timestamp + random.nextInt((int) song.getLength()),
						SongEventType.PAUSE,
						userId));
			}

			Thread.sleep(200);
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}
}
