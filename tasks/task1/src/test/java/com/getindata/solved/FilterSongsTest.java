package com.getindata.solved;

import com.getindata.solved.FilterSongs.SongFilterFunction;
import com.getindata.tutorial.base.model.SongEvent;
import org.junit.jupiter.api.Test;

import static com.getindata.tutorial.base.model.TestDataBuilders.aSong;
import static com.getindata.tutorial.base.model.TestDataBuilders.aSongEvent;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterSongsTest {

    @Test
    void shouldAcceptRelevantSong() {
        SongFilterFunction filterFunction = new SongFilterFunction();

        SongEvent event = aSongEvent()
                .setSong(aSong().author("Adele").build())
                .build();

        assertTrue(filterFunction.filter(event));
    }

    @Test
    void shouldFilterIrrelevantSongs() {
        SongFilterFunction filterFunction = new SongFilterFunction();

        SongEvent event = aSongEvent()
                .setSong(aSong().author("Queen").build())
                .build();

        assertFalse(filterFunction.filter(event));
    }

}