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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.types.Row;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.getindata.tutorial.base.model.SongEvent;

public class SongEventJsonSerializationSchema implements SerializationSchema<SongEvent> {

	private static ObjectMapper mapper = new ObjectMapper();

	public byte[] serialize(SongEvent row) {
		try {
			final ObjectNode json = mapper.createObjectNode()
					.put("song_name", row.getSong().getName())
					.put("song_length", row.getSong().getLength())
					.put("song_author", row.getSong().getAuthor())
					.put("t", row.getTimestamp())
					.put("userId", row.getUserId())
					.put("type", row.getType().toString());
			return mapper.writeValueAsBytes(json);
		} catch (Exception var5) {
			throw new RuntimeException("Failed to serialize row", var5);
		}
	}

	public static TypeInformation<Row> resultingRowInfo = Types.ROW_NAMED(
			new String[]{"song_name", "song_length", "song_author", "t", "userId", "type"},
			Types.STRING, Types.LONG, Types.STRING, Types.LONG, Types.LONG, Types.STRING
	);
}
