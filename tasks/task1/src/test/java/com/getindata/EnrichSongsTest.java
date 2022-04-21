package com.getindata;

import com.getindata.tutorial.base.model.EnrichedSongEvent;
import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Either;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.getindata.EnrichSongs.EnrichmentFunction;
import static com.getindata.tutorial.base.model.TestDataBuilders.aRawSongEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnrichSongsTest {

    private final EnrichmentFunction enrichmentFunction = new EnrichmentFunction();

    @Disabled("Disabled until EnrichmentFunction is implemented")
    @Test
    void shouldEnrichExistingSong() throws Exception {
        // todo: build a test object here
        SongEvent event = aRawSongEvent()
                .build();
        enrichmentFunction.open(new Configuration());

        Either<SongEvent, EnrichedSongEvent> result = enrichmentFunction.map(event);

        assertTrue(result.isRight());
        assertEquals("Paint It Black", result.right().getSong().getName());
    }

    @Disabled("Disabled until EnrichmentFunction is implemented")
    @Test
    void shouldFilterIrrelevantSong() {
        // todo: put your code here
    }

}