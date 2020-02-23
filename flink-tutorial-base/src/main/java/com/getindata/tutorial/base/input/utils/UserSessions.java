/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.getindata.tutorial.base.input.utils;

import com.getindata.tutorial.base.model.SongEvent;

import java.time.Duration;
import java.util.Random;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

@SuppressWarnings("Convert2Lambda")
public class UserSessions {

    private final Stream<SongEvent> songs;

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

    public Stream<SongEvent> getSongs() {
        return songs;
    }
}
