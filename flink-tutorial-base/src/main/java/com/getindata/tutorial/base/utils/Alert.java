package com.getindata.tutorial.base.utils;


import org.joda.time.Duration;
import org.joda.time.Instant;

import static com.getindata.tutorial.base.utils.DurationUtils.formatDuration;

public class Alert {

    private String songName;
    private Instant started;
    private Instant ended;
    private long userId;

    public Alert(String songName, long start, long end, long userId) {
        this.songName = songName;
        this.started = new Instant(start);
        this.ended = new Instant(end);
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "songName='" + songName + '\'' +
                ", userId=" + userId +
                ", started=" + started.toString() +
                ", ended=" + ended.toString() +
                ", duration=" + formatDuration(new Duration(started, ended)) +
                '}';
    }
}
