package com.getindata;

import com.getindata.tutorial.base.input.SongsSource;
import com.getindata.tutorial.base.model.EnrichedSongEvent;
import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

public class EnrichSongs {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // Chaining is disabled for presentation purposes - with chaining enabled job graph in Flink UI is just boring :)
        sEnv.disableOperatorChaining();


        // create a stream of events from source
        final DataStream<Either<SongEvent, EnrichedSongEvent>> events = sEnv
                .addSource(new SongsSource())
                .map(new EnrichmentFunction());

        events.flatMap(new SelectValidEvents())
                .print();

        events.flatMap(new SelectInvalidEvents())
                .map(event -> "Could not enrich event: " + event)
                .print();

        // execute streams
        sEnv.execute("Example program");
    }

    static class EnrichmentFunction extends RichMapFunction<SongEvent, Either<SongEvent, EnrichedSongEvent>> {

        @Override
        public void open(Configuration parameters) throws Exception {
            // todo: fill in the code
        }

        @Override
        public Either<SongEvent, EnrichedSongEvent> map(SongEvent songEvent) throws Exception {
            // todo: fill in the code
            // todo: there is an EnrichmentService class that you can find useful
            return Either.Left(songEvent);
        }
    }

    static class SelectValidEvents implements FlatMapFunction<Either<SongEvent, EnrichedSongEvent>, EnrichedSongEvent> {
        @Override
        public void flatMap(Either<SongEvent, EnrichedSongEvent> event, Collector<EnrichedSongEvent> collector) throws Exception {
            // todo: fill in the code
        }
    }

    static class SelectInvalidEvents implements FlatMapFunction<Either<SongEvent, EnrichedSongEvent>, SongEvent> {
        @Override
        public void flatMap(Either<SongEvent, EnrichedSongEvent> event, Collector<SongEvent> collector) throws Exception {
            // todo: fill in the code
        }
    }
}
