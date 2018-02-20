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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import com.getindata.tutorial.base.es.EsProperties;
import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.model.SongEventType;
import com.getindata.tutorial.base.utils.UserStatistics;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class EsKafkaWindowAggregations {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // create a stream of events from source
    final DataStream<SongEvent> events = sEnv.addSource(
        new FlinkKafkaConsumer011<>(
            KafkaProperties.getTopic("lion"),
            new TypeInformationSerializationSchema<>(
                TypeInformation.of(SongEvent.class),
                sEnv.getConfig()),
            KafkaProperties.getKafkaProperties()
        )
    );

    // assign timestamps and watermark generation
    final DataStream<SongEvent> eventsInEventTime = events.assignTimestampsAndWatermarks(
        new AssignerWithPunctuatedWatermarks<SongEvent>() {
          
          @Override
          public Watermark checkAndGetNextWatermark(SongEvent songEvent, long lastTimestamp) {
            if (songEvent.getUserId() % 2 == 1) {
              return new Watermark(songEvent.getTimestamp());
            } else {
              return new Watermark(songEvent.getTimestamp() - 5*60000);
            }
          }

          @Override
          public long extractTimestamp(SongEvent songEvent, long lastTimestamp) {
            return songEvent.getTimestamp();
          }
        }
    );

    // song plays in user sessions
    final WindowedStream<SongEvent, Integer, TimeWindow> windowedStream = eventsInEventTime
        .filter(new FilterFunction<SongEvent>() {
          @Override
          public boolean filter(final SongEvent songEvent) throws Exception {
            return songEvent.getType() == SongEventType.PLAY;
          }
        })
        .keyBy(new KeySelector<SongEvent, Integer>() {
          @Override
          public Integer getKey(SongEvent songEvent) throws Exception {
            return songEvent.getUserId();
          }
        })
        .window(EventTimeSessionWindows.withGap(Time.minutes(20)));

    final DataStream<UserStatistics> statistics = windowedStream.aggregate(
        // pre-aggregate song plays
        new AggregateFunction<SongEvent, Long, Long>() {
          @Override
          public Long createAccumulator() {
            return 0L;
          }

          @Override
          public Long add(
              SongEvent songEvent, Long count) {
            return count + 1;
          }

          @Override
          public Long getResult(Long count) {
            return count;
          }

          @Override
          public Long merge(
              Long count1, Long count2) {
            return count1 + count2;
          }
        },
        // create user statistics for a session
        new WindowFunction<Long, UserStatistics, Integer, TimeWindow>() {
          @Override
          public void apply(
              Integer userId,
              TimeWindow window,
              Iterable<Long> input,
              Collector<UserStatistics> out) throws Exception {
            long sum = 0;
            for (Long aLong : input) {
              sum += aLong;
            }

            out.collect(
                new UserStatistics(
                    sum,
                    userId,
                    window.getStart(),
                    window.getEnd())
            );
          }
        });

    //write into elasticsearch
    statistics.addSink(
        new ElasticsearchSink<>(EsProperties.getEsProperties(), EsProperties.getEsAddresses(),
            new ElasticsearchSinkFunction<UserStatistics>() {
              private IndexRequest createIndexRequest(UserStatistics element) throws IOException {

                final XContentBuilder result = //TODO fill in the code

                return Requests.indexRequest()
                    .index(EsProperties.getIndex(/*TODO fill in the code*/))
                    .type(EsProperties.getType())
                    .source(result);
              }

              @Override
              public void process(UserStatistics element, RuntimeContext ctx,
                  RequestIndexer indexer) {
                try {
                  indexer.add(createIndexRequest(element));
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            }));

    // execute streams
    sEnv.execute();
  }
}
