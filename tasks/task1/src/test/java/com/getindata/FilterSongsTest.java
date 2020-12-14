package com.getindata;

import com.getindata.tutorial.base.model.EnrichedSongEvent;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.getindata.FilterSongs.SongFilterFunction;
import static com.getindata.tutorial.base.model.TestDataBuilders.aSong;
import static com.getindata.tutorial.base.model.TestDataBuilders.aSongEvent;
import static org.junit.jupiter.api.Assertions.assertTrue;

// FIXME: remove @Disabled
@Disabled
class FilterSongsTest {

    @Test
    void shouldAcceptRelevantSong() {
        SongFilterFunction filterFunction = new SongFilterFunction();

        EnrichedSongEvent event = aSongEvent()
                // TODO build a test object here
                .setSong(aSong().author("TODO").build())
                .build();

        assertTrue(filterFunction.filter(event));
    }

    @Test
    void shouldFilterIrrelevantSongs() {
        // TODO put your code here
    }

}