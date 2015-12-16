package com.gpjpe.helpers;

import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Random;

public final class Utils {

    public static Values tweet() {

        final String[] hashTags = new String[]{"SemperFi", "PewDiePie", "House", "Benzino"};
        final String[] langs = new String[]{"en", "es", "it", "pt"};
        Random random = new Random();

        return new Values(langs[random.nextInt(langs.length)], hashTags[random.nextInt(hashTags.length)],
                System.currentTimeMillis() / 1000L);
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

    public static Long[] calcWindows(long windowLengthSeconds, long windowAdvanceSeconds, long initTimestamp, long timestamp) {

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

        ArrayList<Long> windowsList = new ArrayList<Long>();


        long nextWindowStart = (relTimestamp / windowAdvanceSeconds + 1) * windowAdvanceSeconds;

        for (int i = 0; i < maxWindows; i++) {
            if (!(nextWindowStart - i * windowAdvanceSeconds <= 0)) {
                windowsList.add(nextWindowStart - i * windowAdvanceSeconds);
            }
        }
        return windowsList.toArray(new Long[windowsList.size()]);
    }

    public static String Stringify(Object[] list, String seperator) {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < list.length; i++) {
            sb.append(list[i].toString());
            if (i < list.length - 1) {
                sb.append(seperator);
            }
        }

        return sb.toString();
    }


    public static int calcMaxAmountofWindows(long windowLengthSeconds, long windowAdvanceSeconds) {
        return (int) ((windowLengthSeconds % windowAdvanceSeconds == 0) ?
                windowLengthSeconds / windowAdvanceSeconds :
                windowLengthSeconds / windowAdvanceSeconds + 1);
    }
}
