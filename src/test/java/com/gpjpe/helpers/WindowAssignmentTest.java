package com.gpjpe.helpers;

import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class WindowAssignmentTest extends TestCase{

    private final static Logger LOGGER = LoggerFactory.getLogger(WindowAssignmentTest.class.getName());

    public void testValidWindowAssignmentForAdvanceSmallerSize(){
        LOGGER.info("In function: testValidWindowAssignmentForAdvanceSmallerSize");
        long windowSize = 50;
        long windowAdv = 40;

        long[] validTimestamps = new long[] {10, 20, 30, 40, 50, 60, 70, 81, 110, 120};
        Long[][] windowValues = new Long[][]{
                new Long[]{40L}, //10
                new Long[]{40L}, //20
                new Long[]{40L}, //30
                new Long[]{80L,40L},  //40
                new Long[]{80L},  //50
                new Long[]{80L},  //60
                new Long[]{80L}, //70
                new Long[]{120L, 80L}, //81
                new Long[]{120L}, //110
                new Long[]{160L, 120L}};  //120


        for(int i = 0; i < validTimestamps.length; i++){
            Long[] calculatedWindows = Utils.calcWindows(windowSize,windowAdv, validTimestamps[i]);

            LOGGER.info("Computing windows for timestamp [" + validTimestamps[i] + "]");

            for (Long calculatedWindow : calculatedWindows) {
                LOGGER.info("Calculated " + calculatedWindow);
            }

            assertTrue(Arrays.equals(windowValues[i], calculatedWindows));
        }
    }

    public void testValidWindowAssignmentForAdvanceSmallerSize2(){
        LOGGER.info("In function: testValidWindowAssignmentForAdvanceSmallerSize2");

        long windowSize = 20;
        long windowAdv = 10;

        long[] validTimestamps = new long[] {0, 1, 10, 13, 19, 20, 27, 31};
        Long[][] windowValues = new Long[][]{
                new Long[]{10L}, //0
                new Long[]{10L}, //1
                new Long[]{20L,10L}, //10
                new Long[]{20L,10L},  //13
                new Long[]{20L,10L},  //19
                new Long[]{30L,20L},  //20
                new Long[]{30L,20L},  //27
                new Long[]{40L,30L}}; //31

        for(int i = 0; i < validTimestamps.length; i++){
            Long[] calculatedWindows = Utils.calcWindows(windowSize,windowAdv, validTimestamps[i]);

            LOGGER.info("Computing windows for timestamp [" + validTimestamps[i] + "]");
            for (Long k:calculatedWindows){
                LOGGER.info("Calculated "+k);
            }

            assertTrue(Arrays.equals(windowValues[i], calculatedWindows));
        }
    }

    public void testValidWindowAssignmentForAdvanceSmallerSize3(){
        LOGGER.info("In function: testValidWindowAssignmentForAdvanceSmallerSize3");

        long windowSize = 40;
        long windowAdv = 15;

        long[] validTimestamps = new long[] {0, 1, 15, 29, 40, 44, 50, 67, 81, 1920, 1080, 1452526389};
        Long[][] windowValues = new Long[][]{
                new Long[]{15L}, //0
                new Long[]{15L}, //1
                new Long[]{30L,15L}, //15
                new Long[]{30L,15L},  //29
                new Long[]{45L, 30L}, //40
                new Long[]{45L,30L},  //44
                new Long[]{60L,45L,30L},  //50
                new Long[]{75L,60L,45L},  //67
                new Long[]{90L,75L,60L}, //81
                new Long[]{1935L,1920L,1905L}, //1920
                new Long[]{1095L,1080L,1065L}, //1080
                new Long[]{1452526395L,1452526380L,1452526365L} //1452526389
        };

        for(int i = 0; i < validTimestamps.length; i++){
            Long[] calculatedWindows = Utils.calcWindows(windowSize,windowAdv, validTimestamps[i]);

            LOGGER.info("Computing windows for timestamp [" + validTimestamps[i] + "]");
            for (Long k:calculatedWindows){
                LOGGER.info("Calculated "+k);
            }

            assertTrue(Arrays.equals(windowValues[i], calculatedWindows));
        }
    }


    public void testValidWindowAssignmentForSameSize(){
        LOGGER.info("in function: testValidWindowAssignmentForSameSize");


        long windowSize = 50;
        long windowAdv = 50;

        long[] validTimestamps = new long[] {10, 20, 30, 40, 50, 60, 70, 1452526389L};
        Long[][] windowValues = new Long[][]{
                new Long[]{50L},    //10
                new Long[]{50L},    //20
                new Long[]{50L},    //30
                new Long[]{50L},    //40
                new Long[]{100L},   //50
                new Long[]{100L},   //60
                new Long[]{100L},   //70
                new Long[]{1452526400L}};

        for(int i = 0; i < validTimestamps.length; i++){
            Long[] calculatedWindows = Utils.calcWindows(windowSize,windowAdv, validTimestamps[i]);

            LOGGER.info("Computing windows for timestamp [" + validTimestamps[i] + "]");
            for (Long k:calculatedWindows){
                LOGGER.info("Calculated "+k);
            }

            assertTrue(Arrays.equals(windowValues[i], calculatedWindows));
        }
    }
}
