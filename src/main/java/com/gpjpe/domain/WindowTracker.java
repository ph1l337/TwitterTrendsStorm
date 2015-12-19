package com.gpjpe.domain;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WindowTracker {

    private Long latestFlushedWindow;
    private Long lastTopWindow;
    private int maxWindows;
    private Long advance;

    public WindowTracker(int maxWindows, Long advance) {
        this.latestFlushedWindow = null;
        this.lastTopWindow = null;
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

    public List<Long> update(Long tupleTopWindow) {

        //TODO: is this the very latest window?
        List<Long> windowsToFlush = new ArrayList<>();

        if (this.lastTopWindow == null) {
            this.lastTopWindow = tupleTopWindow;
        }

        Long upperWindow = tupleTopWindow - this.maxWindows * this.advance;
        Long lowerWindow;

        //top most window to be flushed, first time
        if (this.latestFlushedWindow == null) {
            lowerWindow = this.advance;

            if (lowerWindow > 0 && upperWindow > 0) {
                windowsToFlush.add(lowerWindow);
            }
        } else {
            lowerWindow = this.latestFlushedWindow;
        }

        while (upperWindow > lowerWindow) {
            windowsToFlush.add(upperWindow);
            upperWindow -= this.advance;
        }

        if (windowsToFlush.size() > 0) {
            this.latestFlushedWindow = tupleTopWindow - this.maxWindows * this.advance;
        }

        Collections.sort(windowsToFlush);

        this.lastTopWindow = tupleTopWindow;

        return windowsToFlush;
    }
}
