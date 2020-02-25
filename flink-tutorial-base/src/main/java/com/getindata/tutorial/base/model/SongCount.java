package com.getindata.tutorial.base.model;

import java.util.Objects;

public class SongCount {

    private final int userId;
    private final int songCount;


    public SongCount(int userId, int songCount) {
        this.userId = userId;
        this.songCount = songCount;
    }

    public int getUserId() {
        return userId;
    }

    public int getSongCount() {
        return songCount;
    }

    @Override
    public String toString() {
        return "SongCount{" +
                "userId=" + userId +
                ", songCount=" + songCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SongCount songCount1 = (SongCount) o;
        return userId == songCount1.userId &&
                songCount == songCount1.songCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, songCount);
    }
}
