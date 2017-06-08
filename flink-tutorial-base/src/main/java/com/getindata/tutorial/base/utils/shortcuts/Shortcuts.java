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

package com.getindata.tutorial.base.utils.shortcuts;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;

import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.model.SongEventType;

import javax.annotation.Nullable;

public class Shortcuts {

	public static DataStream<SongEvent> getSongsWithTimestamps(StreamExecutionEnvironment sEnv) {

		// create a stream of events from source
		final DataStream<SongEvent> events = sEnv.addSource(
				new FlinkKafkaConsumer010<>(
						"songs",
						new TypeInformationSerializationSchema<>(
								TypeInformation.of(SongEvent.class),
								sEnv.getConfig()),
						KafkaProperties.getKafkaProperties()
				)
		);

		return events.assignTimestampsAndWatermarks(
				new AssignerWithPunctuatedWatermarks<SongEvent>() {
					@Nullable
					@Override
					public Watermark checkAndGetNextWatermark(SongEvent songEvent, long lastTimestamp) {
						return (songEvent.getType() == SongEventType.PLAY) ? new Watermark(lastTimestamp) : null;
					}

					@Override
					public long extractTimestamp(SongEvent songEvent, long lastTimestamp) {
						return songEvent.getTimestamp();
					}
				}
		);
	}

	private Shortcuts() {
	}
}