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

package com.getindata.tutorial.base.input;

import com.getindata.tutorial.base.input.utils.MergedIterator;
import com.getindata.tutorial.base.input.utils.UserSessions;
import com.getindata.tutorial.base.model.SongEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class SongsSource extends RichParallelSourceFunction<SongEvent> {

  private boolean isRunning = true;

  private final int numberOfUsers;

  private final Duration sessionGap;

  private final Duration outOfOrderness;

  private final int speed;

  /**
   * Creates a source that generates {@link SongEvent}s. You can configure with few parameters
   *
   * @param numberOfUsers number of users for which the events will be generated
   * @param sessionGap gap in time between last event in a single user session and the next session
   * @param outOfOrderness time that events for users with event id will be delayed in contrast to other users events
   * @param speed speed of events generation (max 100). The smaller the faster events will be generated
   */
  public SongsSource(int numberOfUsers, Duration sessionGap, Duration outOfOrderness, int speed) {
    this.numberOfUsers = numberOfUsers;
    this.sessionGap = sessionGap;
    this.outOfOrderness = outOfOrderness;
    this.speed = Math.min(Math.max(1, speed), 100);
  }

  public SongsSource() {
    this(10, Duration.ofMinutes(20), Duration.ofMinutes(5), 10);
  }

  @Override
  public void run(SourceContext<SongEvent> sourceContext) throws Exception {
    final List<Iterator<SongEvent>> sessions = IntStream.rangeClosed(1, numberOfUsers)
        .filter(i -> i % getRuntimeContext().getNumberOfParallelSubtasks() == getRuntimeContext()
            .getIndexOfThisSubtask())
        .mapToObj(
            i -> new UserSessions(i, sessionGap, Instant.now().toEpochMilli()).getSongs()
                .iterator())
        .collect(Collectors.toList());

    final MergedIterator<SongEvent> mergedIterator = new MergedIterator<>(sessions,
        Comparator.comparingLong(songEvent -> {
              if (songEvent.getUserId() % 2 == 0) {
                return songEvent.getTimestamp();
              } else {
                return songEvent.getTimestamp() + outOfOrderness.toMillis();
              }
            }
        ));

    while (isRunning & mergedIterator.hasNext()) {
      sourceContext.collect(mergedIterator.next());
      Thread.sleep(20 * speed);
    }

  }

  @Override
  public void cancel() {
    this.isRunning = false;
  }
}
