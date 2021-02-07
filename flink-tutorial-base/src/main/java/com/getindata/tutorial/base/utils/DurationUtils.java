package com.getindata.tutorial.base.utils;


public class DurationUtils {

    public static String formatDuration(long seconds) {
        long absSeconds = Math.abs(seconds);
        String positive = String.format(
                "%d:%02d:%02d",
                absSeconds / 3600,
                (absSeconds % 3600) / 60,
                absSeconds % 60
        );
        return seconds < 0 ? "-" + positive : positive;
    }

    private DurationUtils() {
    }

}
