package com.gpjpe.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import com.gpjpe.helpers.Utils;


public class KafkaTweetsSpout extends BaseRichSpout {

    private final static Logger LOGGER = Logger.getLogger(KafkaTweetsSpout.class.getName());

    private SpoutOutputCollector _collector;
    private Set<String> languagesToWatch;
    private long firstTweetTimestamp;
    private static final long UNSET = -1;

    public KafkaTweetsSpout(String[] languagesToWatch) {
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

    public void nextTuple() {

        if (this.firstTweetTimestamp == UNSET) {
            this.firstTweetTimestamp = LocalDateTime.now().atZone(ZoneId.systemDefault()).toEpochSecond();
        }

        Values tweet = Utils.tweet(this.firstTweetTimestamp);
        String tweetLanguage = (String) tweet.get(0);

        if (this.languagesToWatch.contains(tweetLanguage)) {
            this._collector.emit(tweet);
            LOGGER.info(tweet.toString());
        }else{
            LOGGER.info("Tweet is not of interest");
        }
    }
}
