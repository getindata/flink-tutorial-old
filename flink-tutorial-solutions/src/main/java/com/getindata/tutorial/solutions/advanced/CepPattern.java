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

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.getindata.tutorial.base.model.SongEvent;

import java.util.List;
import java.util.Map;

public class CepPattern {

	public static void main(String[] args) {
		Pattern.<SongEvent>begin("play song")
					.subtype(SongPlayed.class).where(song -> song.getAuthor().equals("Queen"))
				.followedBy("added to playlist")
					.subtype(SongAdded.class).where(song -> song.getAuthor().equals("Queen"))
				.within(Time.seconds(5));

		CEP.pattern().select(new PatternSelectFunction<Object, Object>() {
			@Override
			public Object select(Map<String, List<Object>> map) throws Exception {
				return null;
			}
		});
	}
}
