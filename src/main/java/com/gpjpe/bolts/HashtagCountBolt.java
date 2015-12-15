package com.gpjpe.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.gpjpe.domain.HashtagCount;
import com.gpjpe.domain.HashtagCountComparator;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.StringMatchFilter;

import java.io.*;
import java.util.*;

public class HashtagCountBolt extends BaseRichBolt {

    private static final Logger LOGGER = Logger.getLogger(HashtagCountBolt.class.getName());
    private static final String DEFAULT_OUTPUT_FOLDER = "output";
    private static final int DEFAULT_TOP_HASH_TAG_COUNT = 3;

    private String outputFolder;
    //    private Map<String, Long> hashtagCountMap = new HashMap<String, Long>();
    private Long[] currentWindows;
    private Map<Long, Map<String, Long>> hashTagCountMap;
    private String localLanguage;
    private int maxWindows;
    private String logSuffix;
    private int numberOfTopHashTags;

    private OutputCollector _collector;
    private String _componentId;


//    public HashtagCountBolt() {
//        this(DEFAULT_OUTPUT_FOLDER, 1);
//    }

    public HashtagCountBolt(int numberOfTopHashTags, int maxWindows, String outputFolder, String logSuffix) {
        this.numberOfTopHashTags = numberOfTopHashTags;
        this.outputFolder = outputFolder;
        this.currentWindows = null;
        this.hashTagCountMap = new HashMap<>();
        this.maxWindows = maxWindows;
        this.currentWindows = new Long[maxWindows];
        this.logSuffix = logSuffix;
    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this._componentId = topologyContext.getThisComponentId();
    }

    private void updateHashTagCount(Long window, String hashTag) {

        Map<String, Long> windowCountMap = this.hashTagCountMap.get(window);

        if (windowCountMap == null) {
            windowCountMap = new HashMap<>();
            this.hashTagCountMap.put(window, windowCountMap);
        }

        Long hashTagCount = windowCountMap.get(hashTag);

        if (hashTagCount == null) {
            windowCountMap.put(hashTag, 1L);
        } else {
            windowCountMap.put(hashTag, hashTagCount + 1L);
        }
    }

    private void writeWindowToFile(Long window) {

        Map<String, Long> windowCountMap = this.hashTagCountMap.get(window);

        BufferedWriter writer = null;

        List<HashtagCount> hashTagCountList = new ArrayList<>();

        for (String hashTag : windowCountMap.keySet()) {
            hashTagCountList.add(new HashtagCount(hashTag, windowCountMap.get(hashTag)));
        }

        Collections.sort(hashTagCountList, new HashtagCountComparator());
        Collections.reverse(hashTagCountList);

        try {

            LOGGER.info(
                    String.format("Writing window [%d] to file", window)
            );

            File dir = new File(outputFolder);
            if (!dir.exists()) {
                boolean result = dir.mkdir();
                if (!result) {
                    throw new RuntimeException(
                            String.format("Couldn't create directory [%s] ", outputFolder)
                    );
                }
            }

            writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(
                            String.format("%s/%s_%s.log", outputFolder, localLanguage, logSuffix), true),
                    "utf-8"));


            for (int i = 0; i < numberOfTopHashTags; i++) {
                if (i < hashTagCountList.size()) {
                    writer.write(String.format(
                            "%s,%s,%s,%d",
                            window,
                            localLanguage,
                            hashTagCountList.get(i).getHashtag(),
                            hashTagCountList.get(i).getCount()
                    ));
                } else {
                    writer.write(String.format(
                            "%s,%s,%s,%d",
                            window,
                            localLanguage,
                            "null",
                            0
                    ));
                }
            }

        } catch (IOException e) {
            LOGGER.error(e);
            throw new RuntimeException(e);
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    public void execute(Tuple tuple) {

        //TODO: handle signal to flush

        String tupleLanguage = (String) tuple.getValueByField("lang");
        String tupleHashTag = (String) tuple.getValueByField("hashtag");
        Long[] tupleWindows = (Long[]) tuple.getValueByField("windows");

        //compare local window with tuple
        //if first value changed, flush old window (if size > 1)
        if (this.currentWindows[0] == null) {
            System.arraycopy(tupleWindows, 0, this.currentWindows, 0, tupleWindows.length);
            this.localLanguage = tupleLanguage;
        }

        if (!this.localLanguage.equals(tupleLanguage)) {
            throw new RuntimeException(
                    String.format(
                            "Bolt [%s] has stopped because it received a different tuple language", _componentId
                    )
            );
        }

        if (!this.currentWindows[0].equals(tupleWindows[0])) {

            if (this.currentWindows[maxWindows - 1] != null) {
                //TODO: flush old window
                LOGGER.info("Window changed, flushing last window");

                this.writeWindowToFile(this.currentWindows[maxWindows - 1]);

                //TODO: delete window from hashMap
                this.hashTagCountMap.remove(this.currentWindows[maxWindows - 1]);
            }

            //update current windows
            System.arraycopy(tupleWindows, 0, this.currentWindows, 0, tupleWindows.length);
        }

        //update counts

        for (Long window : tupleWindows) {

            //simply update counts
            this.updateHashTagCount(window, tupleHashTag);
        }

        //TODO: ack tuple?
        this._collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
