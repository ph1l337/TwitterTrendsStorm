package com.gpjpe.helpers;

import junit.framework.TestCase;
import org.apache.log4j.Logger;

public class WindowAssignmentTest extends TestCase{

    private final static Logger LOGGER = Logger.getLogger(WindowAssignmentTest.class.getName());

    public void testValidWindowAssignmentForSmallerAdvance(){

        long windowSize = 30;
        long advance = 10;
        long initTimestamp = 0;

        long[] validTimestamps = new long[]{3, 4, 5, 46, 50, 645};
        long[] windowValues = new long[]{30, 30, 30, 50, 60, 650};

        for(int i = 0; i < validTimestamps.length; i++){
            long calculatedWindow = Utils.calcWindow(windowSize, advance, initTimestamp, validTimestamps[i]);

            LOGGER.info("Computing window size for timestamp [" + validTimestamps[i] + "]");
            assertEquals(windowValues[i], calculatedWindow);
        }
    }

    public void testInvalidWindowAssignmentForSameAdvance(){

//        long windowSize = 30;
//        long advance = 10;
//        long initTimestamp = 0;
//
//        long[] validTimestamps = new long[]{3, 4, 5, 46, 50, 645};
//        long[] windowValues = new long[]{30, 30, 30, 50, 60, 650};
//
//        for(int i = 0; i < validTimestamps.length; i++){
//            long calculatedWindow = Utils.calcWindow(windowSize, advance, initTimestamp, validTimestamps[i]);
//
//            LOGGER.info("Computing window size for timestamp [" + validTimestamps[i] + "]");
//            assertEquals(windowValues[i], calculatedWindow);
//        }
    }

}
