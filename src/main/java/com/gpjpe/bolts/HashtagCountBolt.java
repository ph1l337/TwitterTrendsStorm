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
    private String outputFolder;
    private Map<String, Long> hashtagCountMap = new HashMap<String, Long>();
    private Long currentWindow = null;


    private OutputCollector _collector;


    public HashtagCountBolt() {
        this(DEFAULT_OUTPUT_FOLDER);
    }

    public HashtagCountBolt(String outputFolder) {
        this.outputFolder = outputFolder;
    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        Long tupleWindow = (Long) tuple.getValueByField("window");

        if (currentWindow == null) {
            currentWindow = tupleWindow;
        }

        if (tupleWindow.equals(currentWindow)) {

            String hashtag = tuple.getValueByField("hashtag").toString();
            Long count = hashtagCountMap.get(hashtag);

            if (count == null) {
                hashtagCountMap.put(hashtag, (long) 1);
            } else {
                hashtagCountMap.put(hashtag, count + 1);
            }
        } else {

            BufferedWriter writer = null;
            String lang = tuple.getValueByField("lang").toString();

            Comparator comparator = new HashtagCountComparator();
            List<HashtagCount> hashtagObjectslist = new ArrayList<>();


            for (String key : hashtagCountMap.keySet()) {
                hashtagObjectslist.add(new HashtagCount(key, hashtagCountMap.get(key)));
            }

            Collections.sort(hashtagObjectslist, new HashtagCountComparator());
            Collections.reverse(hashtagObjectslist);


            try {


                LOGGER.info(hashtagCountMap.toString());
                File dir = new File(outputFolder);
                if (!dir.exists()) {
                    boolean result = dir.mkdir();
                    if (!result) {
                        LOGGER.error("Couldn't create directory: " + outputFolder);
                    }
                }

                writer = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(outputFolder + "/" + lang + ".log", true), "utf-8"));
                if (hashtagObjectslist.isEmpty()) {
                    writer.write(currentWindow + "," + "Null" + "," + 0);
                    writer.write(currentWindow + "," + "Null" + "," + 0);
                    writer.write(currentWindow + "," + "Null" + "," + 0);
                    writer.newLine();
                }

                if (hashtagObjectslist.size() == 1) {
                    writer.write(currentWindow + "," + hashtagObjectslist.get(0).getHashtag() + "," + hashtagObjectslist.get(0).getCount());
                    writer.write(currentWindow + "," + "Null" + "," + 0);
                    writer.write(currentWindow + "," + "Null" + "," + 0);
                    writer.newLine();
                }

                if (hashtagObjectslist.size() == 2) {
                    writer.write(currentWindow + "," + hashtagObjectslist.get(0).getHashtag() + "," + hashtagObjectslist.get(0).getCount());
                    writer.write(currentWindow + "," + hashtagObjectslist.get(1).getHashtag() + "," + hashtagObjectslist.get(1).getCount());
                    writer.write(currentWindow + "," + "Null" + "," + 0);
                    writer.newLine();
                }

                if (hashtagObjectslist.size() >= 3) {
                    writer.write(currentWindow + "," + hashtagObjectslist.get(0).getHashtag() + "," + hashtagObjectslist.get(0).getCount());
                    writer.write(currentWindow + "," + hashtagObjectslist.get(1).getHashtag() + "," + hashtagObjectslist.get(1).getCount());
                    writer.write(currentWindow + "," + hashtagObjectslist.get(2).getHashtag() + "," + hashtagObjectslist.get(2).getCount());
                    writer.newLine();
                }

            } catch (IOException e) {
                LOGGER.error(e);
                throw new RuntimeException(e);
            } finally {
                try {
                    writer.close();
                } catch (Exception e) {
                }
            }

            currentWindow = tupleWindow;
            hashtagCountMap.clear();
        }

        _collector.ack(tuple);


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
