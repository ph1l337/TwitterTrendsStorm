package com.gpjpe.domain;

import java.util.Comparator;


public class HashtagCountComparator implements Comparator<HashtagCount> {
    @Override
    public int compare(HashtagCount hashtagCount1, HashtagCount hashtagCount2) {
        if(hashtagCount1.getCount()>hashtagCount2.getCount()){return 1;}
        if(hashtagCount1.getCount().equals(hashtagCount2.getCount())){return 0;}
        return -1;
    }
}
