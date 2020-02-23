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

package com.getindata.tutorial.base.utils;


import org.joda.time.Duration;
import org.joda.time.Instant;

import static com.getindata.tutorial.base.utils.DurationUtils.formatDuration;

public class Alert {

    private String songName;
    private Instant started;
    private Instant ended;
    private long userId;

    public Alert(String songName, long start, long end, long userId) {
        this.songName = songName;
        this.started = new Instant(start);
        this.ended = new Instant(end);
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "songName='" + songName + '\'' +
                ", userId=" + userId +
                ", started=" + started.toString() +
                ", ended=" + ended.toString() +
                ", duration=" + formatDuration(new Duration(started, ended)) +
                '}';
    }
}
