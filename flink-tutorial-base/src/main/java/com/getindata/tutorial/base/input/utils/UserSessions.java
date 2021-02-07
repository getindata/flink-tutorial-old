package com.getindata.tutorial.base.input.utils;

import com.getindata.tutorial.base.model.EnrichedSongEvent;

import java.time.Duration;
import java.util.Random;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

@SuppressWarnings("Convert2Lambda")
public class UserSessions {

    private final Stream<EnrichedSongEvent> songs;

    public UserSessions(int userId, Duration gap, long startTimestamp) {
        final Random random = new Random();
        songs = Stream
                .iterate(new UserSession(userId, random.nextInt(10), startTimestamp),
                        new UnaryOperator<UserSession>() {
                            @Override
                            public UserSession apply(UserSession userSession) {
                                return new UserSession(
                                        userId,
                                        random.nextInt(10),
                                        userSession.getEndTime() + gap.toMillis()
                                );
                            }
                        })
                .flatMap(UserSession::songs);
    }

    public Stream<EnrichedSongEvent> getSongs() {
        return songs;
    }
}
