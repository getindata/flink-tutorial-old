package com.getindata.tutorial.base.utils;


public class DurationUtils {

    public static String formatDuration(long millis) {
        long absSeconds = Math.abs(millis) / 1000;
        String positive = String.format(
                "%d:%02d:%02d",
                absSeconds / 3600,
                (absSeconds % 3600) / 60,
                absSeconds % 60
        );
        return millis < 0 ? "-" + positive : positive;
    }

    private DurationUtils() {
    }

}
