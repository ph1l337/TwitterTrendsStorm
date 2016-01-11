package com.gpjpe.domain;

import java.text.CollationKey;
import java.text.Collator;
import java.util.Comparator;


public class HashtagCountComparator implements Comparator<HashtagCount> {
    @Override
    public int compare(HashtagCount hashtagCount1, HashtagCount hashtagCount2) {
        int hashComparison = hashtagCount1.getCount().compareTo(hashtagCount2.getCount());

        if (hashComparison == 0){
//            return hashtagCount1.getHashtag().compareTo(hashtagCount2.getHashtag())*-1;
            return (Collator.getInstance().compare(hashtagCount2.getHashtag(), hashtagCount1.getHashtag()));
        }

        return  hashComparison;
    }
}
