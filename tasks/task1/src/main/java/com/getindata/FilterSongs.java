package com.getindata;

import com.getindata.tutorial.base.input.SongsSource;
import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterSongs {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a stream of events from source
        final DataStream<SongEvent> events = sEnv
                .addSource(new SongsSource())
                // Chaining is disabled for presentation purposes - with chaining enabled job graph in Flink UI is just boring :)
                .disableChaining()
                .filter(new SongFilterFunction())
                .disableChaining();

        // print filtered events
        events.print();

        // execute streams
        sEnv.execute("Example program");
    }

    static class SongFilterFunction implements FilterFunction<SongEvent> {

        @Override
        public boolean filter(SongEvent songEvent) {
            //TODO fill in the code
            return true;
        }
    }

}
