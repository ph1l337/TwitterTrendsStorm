package com.gpjpe.helpers;

import junit.framework.TestCase;
import org.apache.log4j.Logger;

public class WindowAssignmentTest extends TestCase{

    private final static Logger LOGGER = Logger.getLogger(WindowAssignmentTest.class.getName());

    public void testValidWindowAssignmentForSmallerAdvance(){

        long windowSize = 30;
        long initTimestamp = 0;

        long[] validTimestamps = new long[]{3, 4, 5, 46, 50, 645,-5,-35};
        long[] windowValues = new long[]{30, 30, 30, 60, 60, 660,0,-30};

        for(int i = 0; i < validTimestamps.length; i++){
            long calculatedWindow = Utils.calcWindow(windowSize, initTimestamp, validTimestamps[i]);

            LOGGER.info("Computing window size for timestamp [" + validTimestamps[i] + "]");
            assertEquals(windowValues[i], calculatedWindow);
        }
    }
}
