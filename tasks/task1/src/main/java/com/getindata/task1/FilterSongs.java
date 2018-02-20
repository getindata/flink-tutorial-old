package com.getindata.task1;

import com.getindata.tutorial.base.input.SongsSource;
import com.getindata.tutorial.base.model.SongEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterSongs {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		// create a stream of events from source
		final DataStream<SongEvent> events = sEnv.addSource(new SongsSource())
				// filter events
				.filter(new FilterFunction<SongEvent>() {
					@Override
					public boolean filter(SongEvent songEvent) throws Exception {
            //TODO fill in the code
					  return true;
            //HINT: To access the author of the song, please use: songEvent.getSong().getAuthor()
            //HINT: In java to compare two strings, you need to use .equals(),
            //  e.g. s1.equals("Hello")
					}
				});

		// print filtered events
		events.print();

		// execute streams
		sEnv.execute();
	}
}
