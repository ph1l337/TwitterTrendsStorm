package com.gpjpe.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.gpjpe.helpers.Utils;
import org.apache.log4j.Logger;

import java.util.Map;

public class WindowAssignerBolt extends BaseRichBolt {
    private static final Logger LOGGER = Logger.getLogger(WindowAssignerBolt.class.getName());
    private static final long DEFAULT_WINDOW_LENGTH_S = 300;

    private OutputCollector _collector;
    private final long windowLengthSeconds;

    public WindowAssignerBolt() {
        this(DEFAULT_WINDOW_LENGTH_S);
    }

    public WindowAssignerBolt(long windowLengthSeconds) {
        if (windowLengthSeconds <= 0){
            throw new IllegalArgumentException("0 and negative values not allowed.\n received: " + windowLengthSeconds);
        }
        this.windowLengthSeconds = windowLengthSeconds;

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        long timestamp = (Long) tuple.getValueByField("timestamp");
        long initTimestamp = (Long) tuple.getValueByField("initTimestamp");

        if (timestamp - initTimestamp < 0) {
            LOGGER.warn("Dropped tuple" + tuple.getMessageId());
            return;
        }

        long window = Utils.calcWindow(windowLengthSeconds, initTimestamp, timestamp);
        _collector.emit(new Values(
                tuple.getValueByField("lang"),
                tuple.getValueByField("hashtag"),
                tuple.getValueByField("timestamp"),
                tuple.getValueByField("initTimestamp"),
                window));

        LOGGER.debug("Window Assigned:" + window);

    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("lang", "hashtag", "timestamp", "initTimestamp","window"));
    }
}
