package com.getindata.tutorial.base.model;


import java.time.Instant;

public class SongEvent {
	private Song song;
	private long timestamp;
	private SongEventType type;
	private int userId;

	public SongEvent() {
	}

	public SongEvent(Song song, long timestamp, SongEventType type, int userId) {
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
		return "SongEvent{" +
		       "song=" + song +
		       ", timestamp=" + Instant.ofEpochMilli(timestamp) +
		       ", type=" + type +
		       ", userId=" + userId +
		       '}';
	}
}
