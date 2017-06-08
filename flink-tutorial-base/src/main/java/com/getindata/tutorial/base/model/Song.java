package com.getindata.tutorial.base.model;

public class Song {
	private long length;
	private String name;
	private String author;

	public Song() {
	}

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

	public void setLength(long length) {
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
		       "length=" + length +
		       ", name='" + name + '\'' +
		       ", author='" + author + '\'' +
		       '}';
	}
}
