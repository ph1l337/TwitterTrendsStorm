package com.gpjpe.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.gpjpe.domain.HashtagCount;
import com.gpjpe.domain.HashtagCountComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;


public class HashtagCountBolt extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(HashtagCountBolt.class.getName());
    private static final String DEFAULT_OUTPUT_FOLDER = "output";
    private static final int DEFAULT_TOP_HASH_TAG_COUNT = 3;

    private String outputFolder;
    private Map<String, Map<Long, Map<String, Long>>> languageWindowHashTagCountMap;
    private String logSuffix;
    private int numberOfTopHashTags;
    private Set<String> languages;
    private int _taskId;

    private OutputCollector _collector;

    public HashtagCountBolt(int numberOfTopHashTags, String outputFolder, String logSuffix) {
        this.numberOfTopHashTags = numberOfTopHashTags;
        this.outputFolder = outputFolder;
        this.languageWindowHashTagCountMap = new HashMap<>();
        this.logSuffix = logSuffix;
        this.languages = new HashSet<>();
    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this._taskId = topologyContext.getThisTaskId();
    }

    private void updateHashTagCount(String language, Long window, String hashTag) {

        //windows for a language
        Map<Long, Map<String, Long>> windowHashTagCountMap = this.languageWindowHashTagCountMap.get(language);

        if (windowHashTagCountMap == null) {
            windowHashTagCountMap = new HashMap<>();
            this.languageWindowHashTagCountMap.put(language, windowHashTagCountMap);
        }

        Map<String, Long> hashTagCountMap = windowHashTagCountMap.get(window);

        if (hashTagCountMap == null) {
            hashTagCountMap = new HashMap<>();
            windowHashTagCountMap.put(window, hashTagCountMap);
        }

        Long count = hashTagCountMap.get(hashTag);

        if (count == null) {
            hashTagCountMap.put(hashTag, 1L);
        } else {
            hashTagCountMap.put(hashTag, count + 1L);
        }
    }

    private void writeWindowToFile(Long window) {

        BufferedWriter writer = null;
        List<HashtagCount> hashTagCountList;
        StringBuilder sb;
        Map<Long, Map<String, Long>> windowHashTagCountMap;
        Map<String, Long> hashTagCountMap;

        for (String language : this.languageWindowHashTagCountMap.keySet()) {

            windowHashTagCountMap = this.languageWindowHashTagCountMap.get(language);
            hashTagCountMap = windowHashTagCountMap.get(window);

            sb = new StringBuilder();
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

            try {

                LOGGER.info(
                        String.format("Writing window [%d] for language [%s] to file", window, language)
                );

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
                writer.newLine();

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
        }
    }

    public void execute(Tuple tuple) {

        String tupleLanguage = (String) tuple.getValueByField("lang");
        String tupleHashTag = (String) tuple.getValueByField("hashtag");
        Long[] tupleWindows = (Long[]) tuple.getValueByField("windows");
        Long windowToFlush = (Long) tuple.getValueByField("windowToFlush");

        //handle signal to flush
        if (windowToFlush != null && tupleHashTag == null) {

            this.writeWindowToFile(windowToFlush);

            //delete window from hashMap
            for (String language : this.languageWindowHashTagCountMap.keySet()) {
                LOGGER.info(
                        String.format("Removing window [%s] for language [%s]",
                                windowToFlush,
                                language)
                );

                this.languageWindowHashTagCountMap.get(language).remove(windowToFlush);
            }
        } else {
            //update counts
            if (tupleHashTag != null) {
                for (Long window : tupleWindows) {

                    //simply update counts
                    this.updateHashTagCount(tupleLanguage, window, tupleHashTag);
                }
            }
        }

        if (!this.languages.contains(tupleLanguage)){
            LOGGER.info(
                    String.format(
                            "Task %d has added languague %s, now handling %s",
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
