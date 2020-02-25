package com.getindata.tutorial.base.model;

public final class UserStatisticsBuilder {

    private long start;
    private long end;
    private long userId;
    private long count;

    UserStatisticsBuilder() {
    }

    public UserStatisticsBuilder start(long start) {
        this.start = start;
        return this;
    }

    public UserStatisticsBuilder end(long end) {
        this.end = end;
        return this;
    }

    public UserStatisticsBuilder userId(long userId) {
        this.userId = userId;
        return this;
    }

    public UserStatisticsBuilder count(long count) {
        this.count = count;
        return this;
    }

    public UserStatistics build() {
        return new UserStatistics(userId, count, start, end);
    }
}
