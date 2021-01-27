package com.getindata.tutorial.base.enrichmennt;

import com.getindata.tutorial.base.model.Song;

import java.util.Optional;

import static com.getindata.tutorial.base.input.utils.Songs.SONGS;

public class EnrichmentService {
    /**
     * Fetches song by it's identifier. If song could not be found - returns empty Optional.
     */
    public Optional<Song> getSongById(long songId) {
        for (Song s : SONGS) {
            if (s.getId() == songId) {
                return Optional.of(s);
            }
        }

        return Optional.empty();
    }
}