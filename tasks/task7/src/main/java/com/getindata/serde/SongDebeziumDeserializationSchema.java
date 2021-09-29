package com.getindata.serde;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.getindata.model.SongCdcEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SongDebeziumDeserializationSchema implements DebeziumDeserializationSchema<SongCdcEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(SongDebeziumDeserializationSchema.class);

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<SongCdcEvent> collector) {
        LOG.info("Received CDC event: {}", sourceRecord);
        Struct value = (Struct) sourceRecord.value();

        Struct source = value.getStruct("source");
        long timestamp = source.getInt64("ts_ms");

        SongCdcEvent.Operation operation = SongCdcEvent.Operation.getOperation(value.getString("op"));
        if (operation == SongCdcEvent.Operation.DELETE) {
            Struct before = value.getStruct("before");
            Long id = before.getInt64("id");
            collector.collect(new SongCdcEvent(operation, timestamp, id, null, null));
        } else {
            Struct after = value.getStruct("after");
            Long id = after.getInt64("id");
            String author = after.getString("author");
            String title = after.getString("title");
            collector.collect(new SongCdcEvent(operation, timestamp, id, author, title));
        }
    }

    @Override
    public TypeInformation<SongCdcEvent> getProducedType() {
        return TypeInformation.of(SongCdcEvent.class);
    }
}
