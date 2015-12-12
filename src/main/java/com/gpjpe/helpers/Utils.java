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

}
