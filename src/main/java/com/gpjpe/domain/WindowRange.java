package com.gpjpe.domain;

import com.google.common.collect.Range;

public class WindowRange {

    public Range<Long> getRange() {
        return range;
    }

    public void setRange(Range<Long> range) {
        this.range = range;
    }

    public long getAdvance() {
        return advance;
    }

    public void setAdvance(long advance) {
        this.advance = advance;
    }

    Range<Long> range;
    long advance;

    public WindowRange(Range<Long> range, long advance){
        this.range = range;
        this.advance = advance;
    }

    public static boolean isWithin(long timestamp, long assignedWindow, long windowSize, long advance){

        long lower = assignedWindow - advance;
        long upper = lower + windowSize;

        return timestamp >= lower && timestamp < upper;
    }
}
