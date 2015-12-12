package com.gpjpe.helpers;

import backtype.storm.tuple.Values;

import java.time.LocalDateTime;
import java.time.ZoneId;
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
    public static long calcWindow(long windowLength_s, long windowAdvance_s, long initTimestamp, long timestamp) {

        long window =  windowLength_s;
        long newTimestamp = (timestamp - initTimestamp);

        if ((windowLength_s - newTimestamp) >= 0) {
            window = windowLength_s + (((newTimestamp - windowLength_s) / windowAdvance_s) + 1) * windowAdvance_s;

        }

        return window;
    }

}
