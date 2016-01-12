package com.gpjpe.domain;

import com.google.common.collect.Range;

public class WindowTracker {

    private Long latestFlushedWindow;
    private Long lastTopWindow;
    private int maxWindows;
    private Long advance;
    private Long firstWindow;

    public WindowTracker(int maxWindows, Long advance) {
        this.latestFlushedWindow = null;
        this.lastTopWindow = null;
        this.firstWindow = null;
        this.maxWindows = maxWindows;
        this.advance = advance;
    }

    public Long getLatestFlushedWindow() {
        return latestFlushedWindow;
    }

    public Long getLastTopWindow() {
        return lastTopWindow;
    }

    public int getMaxWindows() {
        return maxWindows;
    }

    public Long getAdvance() {
        return advance;
    }

    public WindowRange update(Long tupleTopWindow) {

        Range<Long> range;
        Long upperWindow;
        Long lowerWindow;

        if (this.firstWindow == null) {
            this.firstWindow = tupleTopWindow;
        }

        if (this.lastTopWindow != null) {
            if (tupleTopWindow.equals(this.lastTopWindow)){
                return new WindowRange(null, this.advance);
            }
        }

        System.out.println("MEASURE" + this.maxWindows + this.advance);

        upperWindow = tupleTopWindow - this.maxWindows * this.advance;

        if (this.firstWindow > upperWindow) {
            range = null;
        } else {
            if (this.latestFlushedWindow == null) {
                lowerWindow = this.firstWindow;
            } else {
                lowerWindow = this.latestFlushedWindow + this.advance;
            }

            if (upperWindow < lowerWindow) {
                return new WindowRange(null, this.advance);
            }

            range = Range.closed(lowerWindow, upperWindow);

            this.latestFlushedWindow = upperWindow;
        }

        this.lastTopWindow = tupleTopWindow;

        return new WindowRange(range, this.advance);
    }
}
