package com.getindata.tutorial.base.model;

public class SongEventBuilder {

    private long songId;
    private long timestamp;
    private SongEventType type;
    private int userId;

    public SongEventBuilder setSongId(long songId) {
        this.songId = songId;
        return this;
    }

    public SongEventBuilder setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public SongEventBuilder setType(SongEventType type) {
        this.type = type;
        return this;
    }

    public SongEventBuilder setUserId(int userId) {
        this.userId = userId;
        return this;
    }

    public SongEvent build() {
        return new SongEvent(songId, timestamp, type, userId);
    }

}
