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
    private static final long DEFAULT_WINDOW_ADVANCE_S = 100;

    private OutputCollector _collector;
    private final long windowLength_s;
    private final long windowAdvance_s;

    public WindowAssignerBolt() {
        this(DEFAULT_WINDOW_LENGTH_S, DEFAULT_WINDOW_ADVANCE_S);
    }

    public WindowAssignerBolt(long windowLength_ms, long windowAdvance_ms) {
        this.windowLength_s = windowLength_ms;
        this.windowAdvance_s = windowAdvance_ms;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        long timestamp = (Long) tuple.getValueByField("timestamp");
        long initTimestamp = (Long) tuple.getValueByField("initTimestamp");

        if (timestamp - initTimestamp < 0) {
            LOGGER.warn("dropped tuple" + tuple.getMessageId());
            return;
        }

        long window = Utils.calcWindow(windowLength_s, windowAdvance_s, initTimestamp, timestamp);
        _collector.emit(new Values(window));
        LOGGER.info("window assigned:"+window);

    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("windowInSeconds"));

    }
}
