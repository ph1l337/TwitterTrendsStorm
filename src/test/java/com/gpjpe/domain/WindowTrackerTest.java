package com.gpjpe.domain;

import com.gpjpe.helpers.Utils;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.List;

public class WindowTrackerTest extends TestCase {

    public void testWindowUpdateForFirstTupleWithinFirstWindow(){

        long windowSize = 30;
        long advance = 10;

        WindowTracker windowTracker = new WindowTracker((int)(windowSize/advance), advance);

        List<Long> windowsToFlush;

        //timestamp 7
        windowsToFlush = windowTracker.update(10L);

        assertTrue(windowsToFlush.isEmpty());
        assertTrue(windowTracker.getLastTopWindow().equals(10L));

        windowsToFlush = windowTracker.update(20L);

        assertTrue(windowsToFlush.isEmpty());
        assertTrue(windowTracker.getLastTopWindow().equals(20L));
    }

    public void testWindowUpdateForFirstTupleToPromptFlush(){

        long windowSize = 30;
        long advance = 10;
        WindowTracker windowTracker;
        List<Long> winToFlush;

        Long[] topWindows = new Long[]{40L, 80L, 120L};
        int[] flushListSizes = new int[]{1, 5, 9};
        Long[] lastTopWindows = new Long[]{40L, 80L, 120L};
        Long[][] windowsToFlush = new Long[][]{
                new Long[]{10L},
                new Long[]{10L, 20L, 30L, 40L, 50L},
                new Long[]{10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L}
        };

        for(int i = 0; i < topWindows.length; i++){
            windowTracker = new WindowTracker((int)(windowSize/advance), advance);
            winToFlush = windowTracker.update(topWindows[i]);
            assertEquals(flushListSizes[i], winToFlush.size());
            assertTrue(windowTracker.getLastTopWindow().equals(lastTopWindows[i]));
            for (int j = 0; j < windowsToFlush[i].length; j++){
                assertEquals(windowsToFlush[i][j], winToFlush.get(j));
            }
        }
    }

    public void testWindowUpdateWithIntervals(){

        long windowSize = 30;
        long advance = 10;

        WindowTracker windowTracker = new WindowTracker((int)(windowSize/advance), advance);

        List<Long> windowsToFlush;
        Long[] expectedWindowsToFlush;

        //first timestamp 7
        windowsToFlush = windowTracker.update(10L);

        assertTrue(windowsToFlush.isEmpty());
        assertTrue(windowTracker.getLastTopWindow().equals(10L));

        //next timestamp 48
        windowsToFlush = windowTracker.update(50L);
        expectedWindowsToFlush = new Long[]{10L, 20L};

        assertTrue(windowTracker.getLastTopWindow().equals(50L));
        assertEquals(expectedWindowsToFlush.length, windowsToFlush.size());
        assertEquals(expectedWindowsToFlush[expectedWindowsToFlush.length - 1], windowTracker.getLatestFlushedWindow());

        for(int i = 0; i < expectedWindowsToFlush.length; i++){
            assertEquals(expectedWindowsToFlush[i], windowsToFlush.get(i));
        }

        //next timestamp 118
        windowsToFlush = windowTracker.update(120L);
        expectedWindowsToFlush = new Long[]{30L, 40L, 50L, 60L, 70L, 80L, 90L};

        assertTrue(windowTracker.getLastTopWindow().equals(120L));
        assertEquals(expectedWindowsToFlush.length, windowsToFlush.size());
        assertEquals(expectedWindowsToFlush[expectedWindowsToFlush.length - 1], windowTracker.getLatestFlushedWindow());

        for(int i = 0; i < expectedWindowsToFlush.length; i++){
            assertEquals(expectedWindowsToFlush[i], windowsToFlush.get(i));
        }
    }
}