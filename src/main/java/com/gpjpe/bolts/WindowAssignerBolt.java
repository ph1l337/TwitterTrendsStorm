package com.gpjpe.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import com.gpjpe.helpers.Utils;

public class WindowAssignerBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(WindowAssignerBolt.class.getName());
    private static final long DEFAULT_WINDOW_LENGTH_S = 300;
    private static final long DEFAULT_WINDOW_ADV_S = 100;

    private OutputCollector _collector;
    private final long windowLengthSeconds;
    private final long windowAdvanceSeconds;

    public WindowAssignerBolt() {
        this(DEFAULT_WINDOW_LENGTH_S, DEFAULT_WINDOW_ADV_S);
    }

    public WindowAssignerBolt(long windowLengthSeconds, long windowAdvanceSeconds) {
        if (windowLengthSeconds <= 0) {
            throw new IllegalArgumentException("0 and negative values not allowed.\n Received: " + windowLengthSeconds);
        }
        if (windowAdvanceSeconds <= 0) {
            throw new IllegalArgumentException("0 and negative values not allowed.\n Received: " + windowAdvanceSeconds);
        }
        this.windowLengthSeconds = windowLengthSeconds;
        this.windowAdvanceSeconds = windowAdvanceSeconds;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        long timestamp = (Long) tuple.getValueByField("timestamp");

        Long[] windows = Utils.calcWindows(windowLengthSeconds,windowAdvanceSeconds, timestamp);
        _collector.emit(new Values(
                tuple.getValueByField("lang"),
                tuple.getValueByField("hashtag"),
                windows));
        this._collector.ack(tuple);
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("lang", "hashtag", "windows"));
    }
}
