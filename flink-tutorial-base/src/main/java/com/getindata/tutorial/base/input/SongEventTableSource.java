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

import com.getindata.tutorial.base.kafka.KafkaProperties;
import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.model.SongEventType;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;
import org.apache.flink.types.Row;

import java.util.List;

import static java.util.Collections.singletonList;

@SuppressWarnings("Convert2Lambda")
public class SongEventTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

    private static final TypeInformation<SongEvent> typeInfo = TypeInformation.of(SongEvent.class);

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        final FlinkKafkaConsumerBase<SongEvent> kafkaSource = new FlinkKafkaConsumer<>(
                KafkaProperties.getTopic("lion"),
                new TypeInformationSerializationSchema<>(
                        typeInfo,
                        env.getConfig()),
                KafkaProperties.getKafkaProperties()
        ).assignTimestampsAndWatermarks(
                new WatermarkStrategy<SongEvent>() {
                    @Override
                    public WatermarkGenerator<SongEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<SongEvent>() {
                            @Override
                            public void onEvent(SongEvent event, long eventTimestamp, WatermarkOutput output) {
                                if (event.getType() == SongEventType.PLAY) {
                                    output.emitWatermark(new Watermark(eventTimestamp));
                                }
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                // don't need to do anything because we emit in reaction to events above
                            }
                        };
                    }

                    @Override
                    public TimestampAssigner<SongEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return (element, recordTimestamp) -> element.getTimestamp();
                    }
                }
        );


        return env.addSource(kafkaSource)
                .map(new MapFunction<SongEvent, Row>() {
                    @Override
                    public Row map(SongEvent songEvent) throws Exception {
                        return Row.of(
                                songEvent.getSong().getName(),
                                songEvent.getSong().getLength(),
                                songEvent.getSong().getAuthor(),
                                songEvent.getUserId(),
                                songEvent.getType().toString());
                    }
                })
                .returns(getReturnType());
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return Types.ROW_NAMED(
                new String[]{"song_name", "song_length", "song_author", "userId", "type"},
                Types.STRING, Types.LONG, Types.STRING, Types.INT, Types.STRING
        );
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.fromTypeInfo(getReturnType());
    }

    @Override
    public String explainSource() {
        return "SongEventTable";
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        RowtimeAttributeDescriptor descriptor = new RowtimeAttributeDescriptor("t", new StreamRecordTimestamp(), new PreserveWatermarks());
        return singletonList(descriptor);
    }
}
