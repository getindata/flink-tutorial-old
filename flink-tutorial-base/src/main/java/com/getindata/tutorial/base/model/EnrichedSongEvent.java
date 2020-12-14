package com.getindata.tutorial.base.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.Instant;

@JsonSerialize
public class EnrichedSongEvent {

    public static EnrichedSongEventBuilder builder() {
        return new EnrichedSongEventBuilder();
    }

    private Song song;
    private long timestamp;
    private SongEventType type;
    private int userId;

    public EnrichedSongEvent() {
    }

    public EnrichedSongEvent(Song song, long timestamp, SongEventType type, int userId) {
        this.song = song;
        this.timestamp = timestamp;
        this.type = type;
        this.userId = userId;
    }

    public Song getSong() {
        return song;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public SongEventType getType() {
        return type;
    }

    public int getUserId() {
        return userId;
    }

    public void setSong(Song song) {
        this.song = song;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setType(SongEventType type) {
        this.type = type;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "EnrichedSongEvent{" +
                "song=" + song +
                ", timestamp=" + Instant.ofEpochMilli(timestamp) +
                ", type=" + type +
                ", userId=" + userId +
                '}';
    }

}
