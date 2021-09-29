package com.getindata.model;


import java.util.Objects;

import static java.lang.String.format;

public class SongCdcEvent {

    public enum Operation {
        INSERT, UPDATE, DELETE;

        public static Operation getOperation(String operation) {
            switch (operation) {
                case "c":
                    return SongCdcEvent.Operation.INSERT;
                case "d":
                    return SongCdcEvent.Operation.DELETE;
                case "u":
                    return SongCdcEvent.Operation.UPDATE;
                default:
                    throw new IllegalArgumentException(format("Unknown operation %s.", operation));
            }
        }
    }

    private Operation operation;
    private long timestamp;
    private long id;
    private String author;
    private String title;

    public SongCdcEvent(Operation operation, long timestamp, long id, String author, String title) {
        this.operation = operation;
        this.timestamp = timestamp;
        this.id = id;
        this.author = author;
        this.title = title;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SongCdcEvent that = (SongCdcEvent) o;
        return timestamp == that.timestamp &&
                id == that.id &&
                operation == that.operation &&
                Objects.equals(author, that.author) &&
                Objects.equals(title, that.title);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operation, timestamp, id, author, title);
    }

    @Override
    public String toString() {
        return "SongCdcEvent{" +
                "operation=" + operation +
                ", timestamp=" + timestamp +
                ", id=" + id +
                ", author='" + author + '\'' +
                ", title='" + title + '\'' +
                '}';
    }
}
