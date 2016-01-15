package com.gpjpe.helpers;

import backtype.storm.tuple.Values;
import com.gpjpe.domain.WindowRange;
import com.gpjpe.domain.WindowTracker;

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

    public static Long[] calcWindows(long windowLengthSeconds, long windowAdvanceSeconds, long timestamp) {

        if (windowLengthSeconds < windowAdvanceSeconds) {
            throw new IllegalArgumentException(String.format("window length mustn't be smaller than window advance" +
                    "received: window length: %d, window advance: %d", windowLengthSeconds, windowLengthSeconds));
        }

        int maxWindows = (int) ((windowLengthSeconds % windowAdvanceSeconds == 0) ?
                windowLengthSeconds / windowAdvanceSeconds :
                windowLengthSeconds / windowAdvanceSeconds + 1);

        ArrayList<Long> windowsList = new ArrayList<Long>();

        long nextWindowStart = (timestamp / windowAdvanceSeconds + 1) * windowAdvanceSeconds;

        for (int i = 0; i < maxWindows; i++) {
            if (!(nextWindowStart - i * windowAdvanceSeconds <= 0)) {

                boolean within = WindowRange.isWithin(timestamp, nextWindowStart - i * windowAdvanceSeconds, windowLengthSeconds, windowAdvanceSeconds);

                if (within) {
                    windowsList.add(nextWindowStart - i * windowAdvanceSeconds);
                }
            }
        }

        return windowsList.toArray(new Long[windowsList.size()]);
    }

    public static String Stringify(Object[] list, String separator) {

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < list.length; i++) {
            sb.append(list[i].toString());
            if (i < list.length - 1) {
                sb.append(separator);
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
