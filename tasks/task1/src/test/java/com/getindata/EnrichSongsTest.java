package com.getindata;

import com.getindata.tutorial.base.model.EnrichedSongEvent;
import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.types.Either;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.getindata.EnrichSongs.EnrichmentFunction;
import static com.getindata.tutorial.base.model.TestDataBuilders.aRawSongEvent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// FIXME: remove @Disabled
@Disabled
class EnrichSongsTest {
    private EnrichmentFunction enrichmentFunction = new EnrichmentFunction();

    @Test
    void shouldEnrichExistingSong() throws Exception {
        // todo: build a test object here
        SongEvent event = aRawSongEvent()
                .build();

        Either<SongEvent, EnrichedSongEvent> result = enrichmentFunction.map(event);

        assertTrue(result.isRight());
        assertEquals("Get Off Of My Cloud", result.right().getSong().getName());
    }

    @Test
    void shouldFilterIrrelevantSong() {
        // todo: put your code here
    }

}