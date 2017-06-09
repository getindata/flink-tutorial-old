package com.getindata.tutorial.base.utils;


import org.joda.time.Duration;
import org.joda.time.Instant;

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

	public Instant getStart() {
		return start;
	}

	public Instant getEnd() {
		return end;
	}

	public UserStatistics(long userId, long count, long start, long end) {
		this.count = count;
		this.userId = userId;
		this.start = new Instant(start);
		this.end = new Instant(end);
		this.duration = new Duration(this.start, this.end);
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