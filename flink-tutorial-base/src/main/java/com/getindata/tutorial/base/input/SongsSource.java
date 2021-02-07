package com.getindata.tutorial.base.input;

import com.getindata.tutorial.base.model.EnrichedSongEvent;
import com.getindata.tutorial.base.model.SongEvent;

/**
 * A source that generates artificial traffic.
 */
public class SongsSource extends SongsSourceBase<SongEvent> {

    @Override
    protected SongEvent mapEvent(EnrichedSongEvent event) {
        return SongEvent.builder()
                .setUserId(event.getUserId())
                .setType(event.getType())
                .setTimestamp(event.getTimestamp())
                .setSongId(event.getSong().getId())
                .build();
    }
}
