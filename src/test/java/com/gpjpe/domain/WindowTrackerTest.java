package com.gpjpe.domain;

import com.google.common.collect.Range;
import junit.framework.TestCase;

public class WindowTrackerTest extends TestCase {

    public void testWindowUpdateForFirstTupleWithinFirstWindow(){

        long windowSize = 30;
        long advance = 10;

        WindowTracker windowTracker = new WindowTracker((int)(windowSize/advance), advance);
        WindowRange windowsToFlush;

        //timestamp 7
        windowsToFlush = windowTracker.update(10L);

        assertNull(windowsToFlush.getRange());
        assertTrue(windowTracker.getLastTopWindow().equals(10L));

        windowsToFlush = windowTracker.update(20L);

        assertNull(windowsToFlush.getRange());
        assertTrue(windowTracker.getLastTopWindow().equals(20L));
    }

    public void testWindowUpdateForFirstTupleWithinFirstWindowWithSameWindowAndAdvance(){

        long windowSize = 20;
        long advance = 20;

        WindowTracker windowTracker = new WindowTracker((int)(windowSize/advance), advance);
        WindowRange windowsToFlush;

        //1452529510‥1452529500
        windowsToFlush = windowTracker.update(100L);
        assertNull(windowsToFlush.getRange());
        assertTrue(windowTracker.getLastTopWindow().equals(100L));

        windowsToFlush = windowTracker.update(120L);
        assertNotNull(windowsToFlush.getRange());
        System.out.print(windowsToFlush.getRange());
        assertEquals(windowsToFlush.getRange().upperEndpoint().compareTo(100L), 0);
        assertEquals(windowsToFlush.getRange().lowerEndpoint().compareTo(100L), 0);

        windowSize = 20;
        advance = 20;
        windowTracker = new WindowTracker((int)(windowSize/advance), advance);

        windowsToFlush = windowTracker.update(100L);
        assertNull(windowsToFlush.getRange());
        assertTrue(windowTracker.getLastTopWindow().equals(100L));

        windowsToFlush = windowTracker.update(160L);
        assertNotNull(windowsToFlush.getRange());
        System.out.print(windowsToFlush.getRange());
        assertEquals(windowsToFlush.getRange().upperEndpoint().compareTo(140L), 0);
        assertEquals(windowsToFlush.getRange().lowerEndpoint().compareTo(100L), 0);
    }


    public void testWindowUpdateForFirstTupleToPromptFlush(){

        long windowSize = 30;
        long advance = 10;
        WindowTracker windowTracker;
        WindowRange winToFlush;

        Long[] topWindows = new Long[]{40L, 80L, 80L, 120L, 120L};
        Long[] firstWindows = new Long[]{10L, 10L, 40L, 10L, 70L};
        Long[] lastTopWindows = new Long[]{40L, 80L, 80L, 120L, 120L};
        Range<Long>[] expectedWindowsToFlush = new Range[]{
                Range.closed(10L, 10L),
                Range.closed(10L, 50L),
                Range.closed(40L, 50L),
                Range.closed(10L, 90L),
                Range.closed(70L, 90L),
        };

        for(int i = 0; i < topWindows.length; i++){
            windowTracker = new WindowTracker((int)(windowSize/advance), advance);
            winToFlush = windowTracker.update(firstWindows[i]);
            assertNull(winToFlush.getRange());
            winToFlush = windowTracker.update(topWindows[i]);
            assertTrue(windowTracker.getLastTopWindow().equals(lastTopWindows[i]));
            assertEquals(expectedWindowsToFlush[i].lowerEndpoint(), winToFlush.getRange().lowerEndpoint());
            assertEquals(expectedWindowsToFlush[i].upperEndpoint(), winToFlush.getRange().upperEndpoint());

        }
    }

    public void testWindowUpdateWithIntervals(){

        long windowSize = 30;
        long advance = 10;

        WindowTracker windowTracker = new WindowTracker((int)(windowSize/advance), advance);

        WindowRange windowsToFlush;
        Range<Long> expectedWindowsToFlush;

        //first timestamp 7
        windowsToFlush = windowTracker.update(10L);

        assertNull(windowsToFlush.getRange());
        assertTrue(windowTracker.getLastTopWindow().equals(10L));

        //next timestamp 48
        windowsToFlush = windowTracker.update(50L);
        expectedWindowsToFlush = Range.closed(10L, 20L);
        assertTrue(windowTracker.getLastTopWindow().equals(50L));
        assertEquals(expectedWindowsToFlush.upperEndpoint(), windowsToFlush.getRange().upperEndpoint());
        assertEquals(expectedWindowsToFlush.lowerEndpoint(), windowsToFlush.getRange().lowerEndpoint());

        //next timestamp 118
        windowsToFlush = windowTracker.update(120L);
        expectedWindowsToFlush = Range.closed(30L, 90L);
        assertTrue(windowTracker.getLastTopWindow().equals(120L));
        assertEquals(expectedWindowsToFlush.upperEndpoint(), windowsToFlush.getRange().upperEndpoint());
        assertEquals(expectedWindowsToFlush.lowerEndpoint(), windowsToFlush.getRange().lowerEndpoint());
    }
}