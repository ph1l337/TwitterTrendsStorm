package com.gpjpe.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.gpjpe.domain.HashtagCount;
import com.gpjpe.domain.HashtagCountComparator;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

public class HashtagCountBolt extends BaseRichBolt {

    private static final Logger LOGGER = Logger.getLogger(HashtagCountBolt.class.getName());
    private static final String DEFAULT_OUTPUT_FOLDER = "output";
    private static final int DEFAULT_TOP_HASH_TAG_COUNT = 3;

    private String outputFolder;
    private Long[] currentWindows;
    private Map<String, Map<Long, Map<String, Long>>> languageWindowHashTagCountMap;
    private int maxWindows;
    private String logSuffix;
    private int numberOfTopHashTags;
    private Set<String> languages;

    private OutputCollector _collector;
    private String _componentId;

    //TODO: assume more than one language present

    public HashtagCountBolt(int numberOfTopHashTags, int maxWindows, String outputFolder, String logSuffix) {
        this.numberOfTopHashTags = numberOfTopHashTags;
        this.outputFolder = outputFolder;
        this.currentWindows = null;
        this.languageWindowHashTagCountMap = new HashMap<>();
        this.maxWindows = maxWindows;
        this.currentWindows = new Long[maxWindows];
        this.logSuffix = logSuffix;
        this.languages = new HashSet<>();
    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        this._componentId = topologyContext.getThisComponentId();
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
        //Is this empty?

        BufferedWriter writer = null;
        List<HashtagCount> hashTagCountList = null;

        for (String language : this.languageWindowHashTagCountMap.keySet()) {

            Map<Long, Map<String, Long>> windowHashTagCountMap = this.languageWindowHashTagCountMap.get(language);

            Map<String, Long> hashTagCountMap = windowHashTagCountMap.get(window);

            //language has no hashtags for this window
            if (hashTagCountMap == null) {
                continue;
            }

            hashTagCountList = new ArrayList<>();

            for (String hashTag : hashTagCountMap.keySet()) {
                hashTagCountList.add(new HashtagCount(hashTag, hashTagCountMap.get(hashTag)));
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
                                String.format("%s/%s_%s.log", outputFolder, language, logSuffix), true),
                        "utf-8"));

                StringBuilder sb = new StringBuilder();

                sb.append(window).append(",").append(language);

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

                writer.write(sb.toString());
                writer.newLine();

            } catch (IOException e) {
                LOGGER.error(e);
                throw new RuntimeException(e);
            } finally {
                try {
                    if (writer != null) {
                        writer.close();
                    }
                } catch (IOException e) {
                    LOGGER.error(e);
                }
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
        }

        if (!this.languages.contains(tupleLanguage)) {
            this.languages.add(tupleLanguage);
            LOGGER.info(
                    String.format(
                            "Bolt [%s] added language [%s]. Now running for languages: %s",
                            _componentId,
                            tupleLanguage,
                            this.languages
                    )
            );
        }

        if (!this.currentWindows[0].equals(tupleWindows[0])) {

            if (this.currentWindows[maxWindows - 1] != null) {
                //flush old window
                LOGGER.info("Window changed, flushing last window");

                //map for this window does not exist?
                this.writeWindowToFile(this.currentWindows[maxWindows - 1]);

                //delete window from hashMap
                for (String language : this.languageWindowHashTagCountMap.keySet()) {
                    this.languageWindowHashTagCountMap.get(language).remove(this.currentWindows[maxWindows - 1]);
                }
            }

            //update current windows
            System.arraycopy(tupleWindows, 0, this.currentWindows, 0, tupleWindows.length);
        }

        //update counts
        if (tupleHashTag != null) {
            for (Long window : tupleWindows) {

                //simply update counts
                this.updateHashTagCount(tupleLanguage, window, tupleHashTag);
            }
        }

        //TODO: ack tuple?
        this._collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
