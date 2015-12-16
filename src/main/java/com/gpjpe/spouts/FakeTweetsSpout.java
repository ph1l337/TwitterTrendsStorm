package com.gpjpe.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.gpjpe.helpers.Utils;


public class FakeTweetsSpout extends BaseRichSpout {

    private final static Logger LOGGER = LoggerFactory.getLogger(FakeTweetsSpout.class.getName());

    private SpoutOutputCollector _collector;
    private Set<String> languagesToWatch;
    private long firstTweetTimestamp;
    private static final long UNSET = -1;

    public FakeTweetsSpout(String[] languagesToWatch) {
        this.languagesToWatch = new HashSet<String>();
        this.languagesToWatch.addAll(Arrays.asList(languagesToWatch));
        this.firstTweetTimestamp = UNSET;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("lang", "hashtag", "timestamp", "initTimestamp"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this._collector = spoutOutputCollector;
    }

    public void close() {

    }

    public void nextTuple() {

        try {

            Values tweet = Utils.tweet();

            if (this.firstTweetTimestamp == UNSET) {
                this.firstTweetTimestamp = (Long) tweet.get(2);
            }

            if (this.languagesToWatch.contains((String) tweet.get(0))) {
                this._collector.emit(
                        new Values(
                                tweet.get(0),
                                tweet.get(1),
                                tweet.get(2),
                                this.firstTweetTimestamp
                        )
                );
            }

        } catch (Exception e) {
            _collector.reportError(e);
            LOGGER.error(e.toString());
        }
    }
}
