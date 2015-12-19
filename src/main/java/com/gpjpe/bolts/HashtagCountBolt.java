package com.gpjpe.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.gpjpe.domain.HashtagCount;
import com.gpjpe.domain.HashtagCountComparator;
import com.gpjpe.domain.WindowHashTagTally;
import com.gpjpe.domain.WindowTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;


public class HashtagCountBolt extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(HashtagCountBolt.class.getName());
    private static final String DEFAULT_OUTPUT_FOLDER = "output";
    private static final int DEFAULT_TOP_HASH_TAG_COUNT = 3;

    private String outputFolder;
    private String logSuffix;
    private int numberOfTopHashTags;
    private Set<String> languages;
    private int _taskId;
    private Map<String, WindowHashTagTally> windowHashTagTallyMap;
    private Map<String, WindowTracker> windowTrackerMap;
    private int maxWindows;
    private long advance;

    private OutputCollector _collector;

    public HashtagCountBolt(int numberOfTopHashTags, int maxWindows, long advance, String outputFolder, String logSuffix) {
        this.numberOfTopHashTags = numberOfTopHashTags;
        this.maxWindows = maxWindows;
        this.advance = advance;
        this.outputFolder = outputFolder;
        this.windowHashTagTallyMap = new HashMap<>();
        this.windowTrackerMap = new HashMap<>();
        this.logSuffix = logSuffix;
        this.languages = new HashSet<>();
    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this._taskId = topologyContext.getThisTaskId();
    }

    private void updateHashTagCount(String language, Long[] windows, String hashTag) {

        WindowHashTagTally windowHashTagTally = this.windowHashTagTallyMap.get(language);

        if (windowHashTagTally == null){
            windowHashTagTally = new WindowHashTagTally();
            this.windowHashTagTallyMap.put(language, windowHashTagTally);
        }

        for(Long window: windows){
            windowHashTagTally.add(hashTag, window);
        }
    }

    private void writeWindowsToFile(List<Long> windowsToFlush, String language) {

        BufferedWriter writer = null;
        StringBuilder sb =  new StringBuilder();
        List<HashtagCount> hashTagCountList;
        Map<String, Long> hashTagCountMap;

        WindowHashTagTally windowHashTagTally = this.windowHashTagTallyMap.get(language);

        for(Long window: windowsToFlush) {

            LOGGER.info(
                    String.format("Writing windows %d for language [%s] to file", window, language)
            );

            hashTagCountMap = windowHashTagTally.getWindowTally(window);
            sb.append(window).append(",").append(language);

            //language has no hashtags for this window
            if (hashTagCountMap == null) {

                for (int i = 0; i < numberOfTopHashTags; i++) {
                    sb.append(",")
                            .append("null")
                            .append(",")
                            .append(0L);
                }

            } else {
                hashTagCountList = new ArrayList<>();

                for (String hashTag : hashTagCountMap.keySet()) {
                    hashTagCountList.add(new HashtagCount(hashTag, hashTagCountMap.get(hashTag)));
                }

                Collections.sort(hashTagCountList, new HashtagCountComparator());
                Collections.reverse(hashTagCountList);

                for (int i = 0; i < numberOfTopHashTags; i++) {
                    if (i < hashTagCountList.size()) {
                        sb.append(",")
                                .append(hashTagCountList.get(i).getHashtag())
                                .append(",")
                                .append(hashTagCountList.get(i).getCount());
                    } else {
                        sb.append(",")
                                .append("null")
                                .append(",")
                                .append(0L);
                    }
                }
            }

            sb.append("\n");
        }

        try {

            File dir = new File(outputFolder);
            if (!dir.exists()) {
                if (!dir.mkdirs()) {
                    throw new RuntimeException(
                            String.format("Couldn't create directory [%s] ", outputFolder)
                    );
                }
            }

            writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(
                            String.format("%s/%s_%s.log", outputFolder, language, logSuffix), true),
                    "utf-8"));

            writer.write(sb.toString());

        } catch (IOException e) {
            LOGGER.error(e.toString());
            throw new RuntimeException(e);
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                LOGGER.error(e.toString());
            }
        }
        for(Long window: windowsToFlush) {
            windowHashTagTally.removeWindow(window);
        }
    }

    public void execute(Tuple tuple) {

        String tupleLanguage = (String) tuple.getValueByField("lang");
        String tupleHashTag = (String) tuple.getValueByField("hashtag");
        Long[] tupleWindows = (Long[]) tuple.getValueByField("windows");

        WindowTracker windowTracker = this.windowTrackerMap.get(tupleLanguage);

        if (windowTracker == null){
            windowTracker = new WindowTracker(this.maxWindows, this.advance);
            this.windowTrackerMap.put(tupleLanguage, windowTracker);
        }

        //drop older tuples
        if (windowTracker.getLastTopWindow() != null){

            if (tupleWindows[0].compareTo(windowTracker.getLastTopWindow()) < 0){
                try {
                    Thread.sleep(1);
                    this._collector.ack(tuple);
                    return;
                } catch (InterruptedException e) {
                    LOGGER.error(e.toString());
                }
            }
        }

        List<Long> windowsToFlush = windowTracker.update(tupleWindows[0]);

        this.writeWindowsToFile(windowsToFlush, tupleLanguage);

        this.updateHashTagCount(tupleLanguage, tupleWindows, tupleHashTag);

        if (!this.languages.contains(tupleLanguage)){
            this.languages.add(tupleLanguage);
            LOGGER.info(
                    String.format(
                            "Task %d has added language %s, now handling %s",
                            this._taskId,
                            tupleLanguage,
                            this.languages
                    )
            );
        }

        //ack tuple
        this._collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
