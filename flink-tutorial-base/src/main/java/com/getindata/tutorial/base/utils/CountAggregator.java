package com.getindata.tutorial.base.utils;

public class CountAggregator {
		private long count = 0;

		public void add(long count) {
			this.count += count;
		}

		public long getCount() {
			return count;
		}

		public CountAggregator() {
		}
	}

