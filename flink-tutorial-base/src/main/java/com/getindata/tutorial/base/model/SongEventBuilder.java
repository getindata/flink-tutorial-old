package com.getindata.tutorial.base.model;

public class SongEventBuilder {

  private Song song;
  private long timestamp;
  private SongEventType type;
  private int userId;

  public SongEventBuilder setSong(Song song) {
    this.song = song;
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

  public SongEvent createSongEvent() {
    return new SongEvent(song, timestamp, type, userId);
  }
}