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

package com.getindata.tutorial.generation;

import com.getindata.tutorial.base.input.SongsSource;
import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class GenerationJob {


  private static final String[] USERS = new String[]{
      "lion",
      "aksolotl",
      "albatross",
      "antelope",
      "beaver",
      "bee",
      "bull",
      "butterfly",
      "camel",
      "deer",
      "dinosaur",
      "dragon",
      "duck",
      "fenek",
      "ferret",
      "flamingo",
      "frog",
      "gazelle",
      "giraffe",
      "hippo",
      "lizard",
      "snake",
      "wolf",
      "zebra",
      "tiger"
  };

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    final DataStream<SongEvent> events = sEnv.addSource(new SongsSource());

    for (String user : USERS) {
      events.addSink(new FlinkKafkaProducer011<>(
          KafkaProperties.getTopic(user),
          new TypeInformationSerializationSchema<>(TypeInformation.of(SongEvent.class),
              sEnv.getConfig()),
          KafkaProperties.getKafkaProperties()));
    }

    sEnv.execute("Kafka producer");
  }
}
