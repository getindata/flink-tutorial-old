package com.getindata.solved;

import com.getindata.tutorial.base.model.EnrichedSongEvent;
import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Either;
import org.junit.jupiter.api.Test;

import static com.getindata.solved.EnrichSongs.EnrichmentFunction;
import static com.getindata.tutorial.base.model.TestDataBuilders.aRawSongEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnrichSongsTest {

    private final EnrichmentFunction enrichmentFunction = new EnrichmentFunction();

    @Test
    void shouldEnrichExistingSong() throws Exception {
        SongEvent event = aRawSongEvent()
                .setSongId(2)
                .build();
        enrichmentFunction.open(new Configuration());

        Either<SongEvent, EnrichedSongEvent> result = enrichmentFunction.map(event);

        assertTrue(result.isRight());
        assertEquals("Get Off Of My Cloud", result.right().getSong().getName());
    }

    @Test
    void shouldFilterIrrelevantSong() throws Exception {
        SongEvent event = aRawSongEvent()
                .setSongId(1000)
                .build();
        enrichmentFunction.open(new Configuration());

        Either<SongEvent, EnrichedSongEvent> result = enrichmentFunction.map(event);

        assertTrue(result.isLeft());
    }

}