package com.getindata.tutorial.base.model;

public final class SongBuilder {

    private int length;
    private String name;
    private String author;

    public SongBuilder length(int length) {
        this.length = length;
        return this;
    }

    public SongBuilder name(String name) {
        this.name = name;
        return this;
    }

    public SongBuilder author(String author) {
        this.author = author;
        return this;
    }

    public Song build() {
        return new Song(length, name, author);
    }
}
