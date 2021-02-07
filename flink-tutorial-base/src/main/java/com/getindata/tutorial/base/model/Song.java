package com.getindata.tutorial.base.model;

public class Song {

    public static SongBuilder builder() {
        return new SongBuilder();
    }

    private long id;
    private int length;
    private String name;
    private String author;

    public Song() {
    }

    public Song(long id, int length, String name, String author) {
        this.id = id;
        this.length = length;
        this.name = name;
        this.author = author;
    }

    public long getId() {
        return id;
    }

    public int getLength() {
        return length;
    }

    public String getName() {
        return name;
    }

    public String getAuthor() {
        return author;
    }

    public void setId(long id) {
        this.id = id;
    }
    public void setLength(int length) {
        this.length = length;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    @Override
    public String toString() {
        return "Song{" +
                "id=" + id +
                ", length=" + length +
                ", name='" + name + '\'' +
                ", author='" + author + '\'' +
                '}';
    }

}
