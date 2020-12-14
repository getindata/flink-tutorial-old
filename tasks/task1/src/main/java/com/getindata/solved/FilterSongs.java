package com.getindata.solved;

import com.getindata.tutorial.base.enrichmennt.EnrichmentService;
import com.getindata.tutorial.base.input.SongsSource;
import com.getindata.tutorial.base.model.EnrichedSongEvent;
import com.getindata.tutorial.base.model.Song;
import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

import java.util.Optional;

public class FilterSongs {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a stream of events from source
        final DataStream<Either<SongEvent, EnrichedSongEvent>> events = sEnv
                .addSource(new SongsSource())
                // Chaining is disabled for presentation purposes - with chaining enabled job graph in Flink UI is just boring :)
                .disableChaining()
                .map(new EnrichmentFunction());

        events.flatMap(new SelectValidEvents())
                .filter(new SongFilterFunction())
                .print();

        events.flatMap(new SelectInvalidEvents())
                .map(event -> "Could not enrich event: " + event)
                .print();

        // execute streams
        sEnv.execute("Example program");
    }

    static class SongFilterFunction implements FilterFunction<EnrichedSongEvent> {

        @Override
        public boolean filter(EnrichedSongEvent songEvent) {
            return songEvent.getSong().getAuthor().equals("Abba") || songEvent.getSong().getAuthor().equals("Adele");
        }
    }

    static class SelectValidEvents implements FlatMapFunction<Either<SongEvent, EnrichedSongEvent>, EnrichedSongEvent> {
        @Override
        public void flatMap(Either<SongEvent, EnrichedSongEvent> event, Collector<EnrichedSongEvent> collector) throws Exception {
            if (event.isRight()) {
                collector.collect(event.right());
            }
        }
    }

    static class SelectInvalidEvents implements FlatMapFunction<Either<SongEvent, EnrichedSongEvent>, SongEvent> {
        @Override
        public void flatMap(Either<SongEvent, EnrichedSongEvent> event, Collector<SongEvent> collector) throws Exception {
            if (event.isLeft()) {
                collector.collect(event.left());
            }
        }
    }

    static class EnrichmentFunction extends RichMapFunction<SongEvent, Either<SongEvent, EnrichedSongEvent>> {

        private transient EnrichmentService service;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            service = new EnrichmentService();
        }

        @Override
        public Either<SongEvent, EnrichedSongEvent> map(SongEvent songEvent) throws Exception {
            Optional<Song> song = service.getSongById(songEvent.getSongId());

            if (!song.isPresent()) {
                return Either.Left(songEvent);
            } else {
                return Either.Right(EnrichedSongEvent.builder()
                        .setSong(song.get())
                        .setTimestamp(songEvent.getTimestamp())
                        .setType(songEvent.getType())
                        .setUserId(songEvent.getUserId())
                        .build());
            }
        }
    }
}
