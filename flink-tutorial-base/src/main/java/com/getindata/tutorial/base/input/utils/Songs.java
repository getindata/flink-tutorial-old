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

import com.getindata.tutorial.base.model.Song;
import org.joda.time.Duration;

import java.util.List;

import static org.apache.flink.shaded.guava18.com.google.common.collect.Lists.newArrayList;

class Songs {

    static final List<Song> SONGS = newArrayList(
            new Song(toMillis(2, 40), "Yellow Submarine", "The Beatles"),
            new Song(toMillis(2, 59), "Get Off Of My Cloud", "The Rolling Stones"),
            new Song(toMillis(5, 28), "Let It Bleed", "The Rolling Stones"),
            new Song(toMillis(3, 51), "Dancing Queen", "Abba"),
            new Song(toMillis(3, 53), "Rolling in the Deep", "Adele"),
            new Song(toMillis(3, 11), "Killer Queen", "Queen"),
            new Song(toMillis(3, 54), "California Gurls", "Katy Perry"),
            new Song(toMillis(4, 57), "Silent All These Years", "Tori Amos"),
            new Song(toMillis(6, 6), "Bohemian Rhapsody", "Queen"),
            new Song(toMillis(4, 32), "I want to break free", "Queen")
    );

    private static int toMillis(int minutes, int seconds) {
        return (int) Duration.standardMinutes(minutes).plus(Duration.standardSeconds(seconds)).getMillis();
    }

    private Songs() {
    }
}
