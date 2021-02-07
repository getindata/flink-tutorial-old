package com.getindata.tutorial.base.input;

import com.getindata.tutorial.base.model.EnrichedSongEvent;

/**
 * A source that generates artificial traffic.
 */
public class EnrichedSongsSource extends SongsSourceBase<EnrichedSongEvent> {

    @Override
    protected EnrichedSongEvent mapEvent(EnrichedSongEvent event) {
        return event;
    }
}
