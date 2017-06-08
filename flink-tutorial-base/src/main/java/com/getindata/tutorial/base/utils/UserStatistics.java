package com.getindata.tutorial.base.utils;

import java.time.Duration;
import java.time.Instant;

import static com.getindata.tutorial.base.utils.DurationUtils.formatDuration;

public class UserStatistics {
	private long userId;
	private long count;
	private Instant start;
	private Instant end;
	private Duration duration;

	public long getUserId() {
		return userId;
	}

	public long getCount() {
		return count;
	}

	public Duration getDuration() {
		return duration;
	}

	public Instant getStart() {
		return start;
	}

	public Instant getEnd() {
		return end;
	}

	public UserStatistics(long userId, long count, Instant start, Instant end) {
		this.count = count;
		this.userId = userId;
		this.start = start;
		this.end = end;
		this.duration = Duration.between(start, end);
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public void setStart(Instant start) {
		this.start = start;
	}

	public void setEnd(Instant end) {
		this.end = end;
	}

	public void setDuration(Duration duration) {
		this.duration = duration;
	}

	@Override
	public String toString() {
		return "UserStatistics{" +
		       "userId=" + userId +
		       ", count=" + count +
		       ", start=" + start +
		       ", end=" + end +
		       ", duration=" + formatDuration(duration) +
		       '}';
	}
}