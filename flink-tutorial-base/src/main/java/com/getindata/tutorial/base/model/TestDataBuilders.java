package com.getindata.tutorial.base.model;

public class TestDataBuilders {

    public static SongBuilder aSong() {
        return new SongBuilder()
                .id(1)
                .length(100)
                .name("Some Song")
                .author("John Doe");
    }

    public static EnrichedSongEventBuilder aSongEvent() {
        return new EnrichedSongEventBuilder()
                .setSong(aSong().build())
                .setTimestamp(1000L)
                .setType(SongEventType.PLAY)
                .setUserId(10);
    }

    public static SongBuilder aRollingStonesSong() {
        return new SongBuilder()
                .length(100)
                .name("Paint It, Black")
                .author("The Rolling Stones");
    }

    public static EnrichedSongEventBuilder aRollingStonesSongEvent() {
        return aSongEvent()
                .setSong(aRollingStonesSong().build());
    }

}
