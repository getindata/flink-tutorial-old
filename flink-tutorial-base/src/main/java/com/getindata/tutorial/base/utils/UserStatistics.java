package com.getindata.tutorial.base.utils;

import java.time.Duration;
import java.time.Instant;

public class UserStatistics {
	private final long userId;
	private final long count;
	private final Instant start;
	private final Instant end;
	private final Duration duration;

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

	private static String formatDuration(Duration duration) {
		long seconds = duration.getSeconds();
		long absSeconds = Math.abs(seconds);
		String positive = String.format(
				"%d:%02d:%02d",
				absSeconds / 3600,
				(absSeconds % 3600) / 60,
				absSeconds % 60);
		return seconds < 0 ? "-" + positive : positive;
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