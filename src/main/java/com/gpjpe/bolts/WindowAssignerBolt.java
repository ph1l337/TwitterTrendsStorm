package com.gpjpe.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import jline.internal.Log;
import org.apache.log4j.Logger;

import java.util.Map;

public class WindowAssignerBolt extends BaseRichBolt{
    private static final Logger LOGGER = Logger.getLogger(WindowAssignerBolt.class.getName());
    private static final int DEFAULT_WINDOW_LENGTH_S = 300;
    private static final int DEFAULT_WINDOW_ADVANCE_S = 100;

    private OutputCollector collector;
    private final int windowLength_s;
    private final int windowAdvance_s;

    public WindowAssignerBolt(){this(DEFAULT_WINDOW_LENGTH_S,DEFAULT_WINDOW_ADVANCE_S)}
    public WindowAssignerBolt(int windowLength_ms, int windowAdvance_ms){
        this.windowLength_s=windowLength_ms;
        this.windowAdvance_s=windowAdvance_ms;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=outputCollector;
    }

    public void execute(Tuple tuple) {
        Long timestamp = (Long) tuple.getValueByField("timestamp");
        Long initTimestamp = (Long) tuple.getValueByField("initTimestamp");

        //TODO change name of @newTimestamp
        int newTimestamp = (int) (timestamp - initTimestamp);
        int window = windowLength_s;
        if((windowLength_s-newTimestamp)>=0) {
            window = windowLength_s + (((newTimestamp - windowLength_s) / windowAdvance_s)+1) * windowAdvance_s;
        }

        Log.info("window assigned" + window);




    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
