//package com.getindata.tutorial.solutions.basic;
//
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import com.getindata.tutorial.base.input.SongsSource;
//import com.getindata.tutorial.base.model.SongEvent;
//
//public class FilterSongs {
//	public static void main(String[] args) throws Exception {
//		final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		// create a stream of events from source
//		final DataStream<SongEvent> events = sEnv.addSource(new SongsSource());
//
//		// filter events
//		final DataStream<SongEvent> filteredEvents = events.filter(/* INSERT YOUR CODE HERE */);
//
//		// print filtered events
//		filteredEvents.print();
//
//		// execute streams
//		sEnv.execute();
//	}
//}
