package com.getindata.tutorial.base.input;

import com.getindata.tutorial.base.input.SongEventJsonSerializationSchema;
import com.getindata.tutorial.base.model.Song;
import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.model.SongEventType;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;


public class SongEventJsonSerializationSchemaTest {

	@Test
	public void serialize() {
		final SongEvent event = new SongEvent(new Song(123213L, "sads", "sdas"), 213L, SongEventType.PLAY, 1);
		final byte[] serialized = new SongEventJsonSerializationSchema().serialize(event);
		assertEquals(
				"{\"song_name\":\"sads\",\"song_length\":123213,\"song_author\":\"sdas\",\"t\":213,\"userId\":1,\"type\":\"PLAY\"}",
				new String(serialized, StandardCharsets.UTF_8));
	}

}