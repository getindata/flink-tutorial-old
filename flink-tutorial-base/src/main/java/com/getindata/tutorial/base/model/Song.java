package com.getindata.tutorial.base.model;

public class Song {
	private final long length;
	private final String name;
	private final String author;

	public Song(long length, String name, String author) {
		this.length = length;
		this.name = name;
		this.author = author;
	}

	public long getLength() {
		return length;
	}

	public String getName() {
		return name;
	}

	public String getAuthor() {
		return author;
	}

	@Override
	public String toString() {
		return "Song{" +
		       "length=" + length +
		       ", name='" + name + '\'' +
		       ", author='" + author + '\'' +
		       '}';
	}
}
