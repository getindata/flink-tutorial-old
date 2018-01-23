
package com.getindata.tutorial.base.input.utils;

import static com.getindata.tutorial.base.input.utils.Songs.SONGS;

import com.getindata.tutorial.base.model.SongEvent;
import com.getindata.tutorial.base.model.SongEventBuilder;
import com.getindata.tutorial.base.model.SongEventType;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.flink.shaded.com.google.common.collect.Lists;

public class UserSession {

  private final List<SongEvent> events;

  private long endTime;

  public UserSession(int userId, int numberOfSongs, long startTimestamp) {
    final Random random = new Random();
    this.endTime = startTimestamp;
    this.events = IntStream.rangeClosed(1, numberOfSongs).mapToObj(
        i -> SONGS.get(random.nextInt(SONGS.size()))
    ).sequential().flatMap(s -> {
      final SongEventBuilder songEventBuilder = new SongEventBuilder();
      songEventBuilder.setSong(s);
      songEventBuilder.setUserId(userId);

      final double stopChoose = random.nextDouble();
      boolean wasStopped = stopChoose > 0.8;
      boolean wasSkipped = stopChoose > 0.6 && stopChoose <= 0.8;

      final ArrayList<SongEvent> events = Lists
          .newArrayList(songEventBuilder.setType(SongEventType.PLAY).createSongEvent());

      if (wasStopped) {
        events.add(songEventBuilder.setType(SongEventType.PAUSE).createSongEvent());
      }

      if (wasSkipped) {
        events.add(songEventBuilder.setType(SongEventType.SKIP).createSongEvent());
      }

      return events.stream();
    }).sequential().map(new Function<SongEvent, SongEvent>() {

      @Override
      public SongEvent apply(SongEvent songEvent) {
        if (songEvent.getType() == SongEventType.PLAY) {
          songEvent.setTimestamp(endTime);
          endTime = endTime + songEvent.getSong().getLength();
          return songEvent;
        } else {
          final int whenSkipped = random.nextInt(songEvent.getSong().getLength());

          endTime = endTime - whenSkipped;
          songEvent.setTimestamp(endTime);
          return songEvent;
        }
      }
    }).collect(Collectors.toList());

  }

  public long getEndTime() {
    return endTime;
  }

  public Stream<SongEvent> songs() {
    return events.stream();
  }
}
