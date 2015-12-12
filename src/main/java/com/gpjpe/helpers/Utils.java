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
    public static long calcWindow(long windowLength_s, long initTimestamp, long timestamp) {

        long window;
        long newTimestamp = (timestamp - initTimestamp);

        if(newTimestamp>=0){
            window = ((newTimestamp/windowLength_s)+1)*windowLength_s;
        }else{
            window = (newTimestamp/windowLength_s)*windowLength_s;
        }

        return window;
    }


   /* public static long[] calcWindow(long windowLength_s, long windowAdvance_s, long initTimestamp, long timestamp) {

        long window[];
        long newTimestamp = (timestamp - initTimestamp);

        if(newTimestamp>=0){
            window = ((newTimestamp/windowLength_s)+1)*windowLength_s;
        }else{
            window = (newTimestamp/windowLength_s)*windowLength_s;
        }

        return window;
    }*/

}
