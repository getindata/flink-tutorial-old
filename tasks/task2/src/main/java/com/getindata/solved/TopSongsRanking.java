package com.getindata.solved;

import com.getindata.tutorial.base.input.EnrichedSongsSource;
import com.getindata.tutorial.base.model.EnrichedSongEvent;
import com.getindata.tutorial.base.model.Song;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import static java.util.Comparator.comparing;

public class TopSongsRanking {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a stream of events from source
        final DataStream<EnrichedSongEvent> events = sEnv.addSource(new EnrichedSongsSource());
        // In order not to copy the whole pipeline code from production to test, we made sources and sinks pluggable in
        // the production code so that we can now inject test sources and test sinks in the tests.
        final DataStream<SongsRanking> statistics = pipeline(events);

        // print results
        statistics.print();

        // execute streams
        sEnv.execute();
    }

    static DataStream<SongsRanking> pipeline(DataStream<EnrichedSongEvent> source) {
        return source
                .assignTimestampsAndWatermarks(new SongWatermarkStrategy())
                .keyBy(new SongKeySelector())
                .window(TumblingEventTimeWindows.of(Time.hours(1L)))
                .aggregate(new SongAggregationFunction(), new SongWindowFunction())
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1L)))
                .process(new TopNSongsFunction(3));
    }

    static class SongWatermarkStrategy implements WatermarkStrategy<EnrichedSongEvent> {

        private static final long FIVE_MINUTES = 5 * 1000 * 60L;

        @Override
        public WatermarkGenerator<EnrichedSongEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<EnrichedSongEvent>() {
                @Override
                public void onEvent(EnrichedSongEvent songEvent, long eventTimestamp, WatermarkOutput output) {
                    Watermark watermark = songEvent.getUserId() % 2 == 1
                            ? new Watermark(songEvent.getTimestamp())
                            : new Watermark(songEvent.getTimestamp() - FIVE_MINUTES);
                    output.emitWatermark(watermark);
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    // don't need to do anything because we emit in reaction to events above
                }
            };
        }

        @Override
        public TimestampAssigner<EnrichedSongEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (element, recordTimestamp) -> element.getTimestamp();
        }
    }

    static class SongKeySelector implements KeySelector<EnrichedSongEvent, Song> {
        @Override
        public Song getKey(EnrichedSongEvent songEvent) {
            return songEvent.getSong();
        }
    }

    static class SongAggregationFunction implements AggregateFunction<EnrichedSongEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(EnrichedSongEvent songEvent, Long count) {
            return count + 1;
        }

        @Override
        public Long getResult(Long count) {
            return count;

        }

        @Override
        public Long merge(Long count1, Long count2) {
            return count1 + count2;
        }

    }

    static class SongWindowFunction implements WindowFunction<Long, SongAndCount, Song, TimeWindow> {
        @Override
        public void apply(Song song, TimeWindow window, Iterable<Long> input, Collector<SongAndCount> out) {
            long sum = 0;
            for (Long l : input) {
                sum += l;
            }

            out.collect(new SongAndCount(song, sum));
        }
    }

    static class TopNSongsFunction extends ProcessAllWindowFunction<SongAndCount, SongsRanking, TimeWindow> {

        private final int n;

        TopNSongsFunction(int n) {
            this.n = n;
        }

        @Override
        public void process(Context context,
                            Iterable<SongAndCount> elements,
                            Collector<SongsRanking> out) throws Exception {
            // Iterate over elements and keep on-line topN elements in a priority queue
            // This is more efficient than loading all elements and sorting all of them.
            PriorityQueue<SongAndCount> topNElementsQueue = new PriorityQueue<>(
                    comparing(SongAndCount::getCount)
            );
            elements.forEach(element -> {
                if (topNElementsQueue.size() < this.n) {
                    topNElementsQueue.add(element);
                } else if (topNElementsQueue.peek().getCount() < element.getCount()) {
                    topNElementsQueue.poll();
                    topNElementsQueue.add(element);
                }
            });

            List<SongAndCount> topNElements = new ArrayList<>(topNElementsQueue);
            Collections.reverse(topNElements);

            out.collect(new SongsRanking(context.window(), topNElements));
        }
    }

    static class SongAndCount {
        private Song song;
        private long count;

        public SongAndCount(Song song, long count) {
            this.song = song;
            this.count = count;
        }

        public Song getSong() {
            return song;
        }

        public long getCount() {
            return count;
        }
    }

    static class SongsRanking {
        private TimeWindow timeWindow;
        private List<SongAndCount> topSongs;

        public SongsRanking(TimeWindow timeWindow, List<SongAndCount> topSongs) {
            this.timeWindow = timeWindow;
            this.topSongs = topSongs;
        }

        public TimeWindow getTimeWindow() {
            return timeWindow;
        }

        public List<SongAndCount> getTopSongs() {
            return topSongs;
        }
    }
}
