package com.gpjpe.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class KafkaTweetsSpout extends BaseRichSpout {

//    private final String[] hashTags = new String[]{"SemperFi", "PewDiePie", "House", "Benzino"};

    SpoutOutputCollector _collector;
    Set<String> languages;


    public KafkaTweetsSpout(String[] languages){
        this.languages.addAll(Arrays.asList(languages));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this._collector = spoutOutputCollector;
    }

    public void nextTuple() {
//        String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
//                "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
//        String sentence = sentences[_rand.nextInt(sentences.length)];

        //read from kafka


//        _collector.emit(new Values(sentence));



    }
}
