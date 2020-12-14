package com.getindata.tutorial.base.model;

public final class SongBuilder {

    private long id;
    private int length;
    private String name;
    private String author;

    public SongBuilder id(long id) {
        this.id = id;
        return this;
    }

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
        return new Song(id, length, name, author);
    }
}
