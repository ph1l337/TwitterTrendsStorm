package com.gpjpe.helpers;

import backtype.storm.tuple.Values;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class Utils {

    public static Values tweet(long initTimestamp) {

        final String[] hashTags = new String[]{"SemperFi", "PewDiePie", "House", "Benzino"};
        final String[] langs = new String[]{"en", "es", "it", "pt"};
        Random random = new Random();

        return new Values(langs[random.nextInt(langs.length)], hashTags[random.nextInt(hashTags.length)],
                LocalDateTime.now().atZone(ZoneId.systemDefault()).toEpochSecond(), initTimestamp);
    }

    //todo enusre initTimestamp > timestamp
    public static long calcWindow(long windowLength_s, long initTimestamp, long timestamp) {

        long window;
        long newTimestamp = (timestamp - initTimestamp);

        if (newTimestamp >= 0) {
            window = ((newTimestamp / windowLength_s) + 1) * windowLength_s;
        } else {
            window = (newTimestamp / windowLength_s) * windowLength_s;
        }

        return window;
    }

    public static long[] calcWindows(long windowLengthSeconds, long windowAdvanceSeconds, long initTimestamp, long timestamp) {

        if (windowLengthSeconds < windowAdvanceSeconds) {
            throw new IllegalArgumentException(String.format("window length mustn't be smaller than window advance" +
                    "received: window length: %d, window advance: %d", windowLengthSeconds, windowLengthSeconds));
        }

        if (initTimestamp > timestamp) {
            throw new IllegalArgumentException(String.format("initTimestamp mustn't be higher than current timestamp" +
                    "received: window length: %d, window advance: %d", windowLengthSeconds, windowAdvanceSeconds));
        }

        long relTimestamp = (timestamp - initTimestamp);

        int maxWindows = (int) ((windowLengthSeconds % windowAdvanceSeconds == 0) ?
                windowLengthSeconds / windowAdvanceSeconds :
                windowLengthSeconds / windowAdvanceSeconds + 1);

        List<Long> windows = new ArrayList<>();

       // long[] windows = new long[maxWindows];


        long nextWindowStart = (relTimestamp / windowAdvanceSeconds + 1) * windowAdvanceSeconds;

        for (int i = maxWindows; i > 0; i--) {
            if (!(nextWindowStart - i * windowAdvanceSeconds <= 0)) {
                windows.add(i,nextWindowStart - i * windowAdvanceSeconds);
            }
        }

        return windows;
    }


}
