package com.getindata.tutorial.base.model;

public class EnrichedSongEventBuilder {

    private Song song;
    private long timestamp;
    private SongEventType type;
    private int userId;

    public EnrichedSongEventBuilder setSong(Song song) {
        this.song = song;
        return this;
    }

    public EnrichedSongEventBuilder setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public EnrichedSongEventBuilder setType(SongEventType type) {
        this.type = type;
        return this;
    }

    public EnrichedSongEventBuilder setUserId(int userId) {
        this.userId = userId;
        return this;
    }

    public EnrichedSongEvent build() {
        return new EnrichedSongEvent(song, timestamp, type, userId);
    }

}
