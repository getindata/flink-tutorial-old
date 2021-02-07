package com.getindata.tutorial.base.enrichmennt;

import com.getindata.tutorial.base.model.Song;

import java.util.Optional;

import static com.getindata.tutorial.base.input.utils.Songs.SONGS;

public class EnrichmentService {
    /**
     * Fetches song by its identifier. If song could not be found - returns empty Optional.
     */
    public Optional<Song> getSongById(long songId) {
        return Optional.ofNullable(SONGS.get(songId));
    }
}