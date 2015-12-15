package com.gpjpe.bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;
import org.apache.log4j.Logger;

public class NewWindowNotifierBolt extends BaseRichBolt {
    private static final Logger LOGGER = Logger.getLogger(NewWindowNotifierBolt.class.getName());
    private OutputCollector _collector;
    String[] langs;
    private Long mostRecentWindow = null;

    public NewWindowNotifierBolt(String[] langs) {
        this.langs = langs;
    }


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        Long tupleWindow = ((Long[]) (tuple.getValueByField("windows")))[0];

        //always pass on the tuple received.
        _collector.emit(new Values(
                tuple.getValueByField("lang"),
                tuple.getValueByField("hashtag"),
                tuple.getValueByField("windows")));

        LOGGER.debug("sent tuple:{"+tuple.getValueByField("lang")+ tuple.getValueByField("hashtag")+tuple.getValueByField("windows").toString());


        if (mostRecentWindow == null) {
            mostRecentWindow = tupleWindow;
        }

        //if a with a new most recent window arrives sent out tuples with hashtag=null to every language in question

        if (!mostRecentWindow.equals(tupleWindow)) {
            for (String lang : langs) {
                if (!lang.equals(tuple.getValueByField("lang"))) {
                    _collector.emit(new Values(
                            lang,
                            null,
                            tuple.getValueByField("windows")));
                    LOGGER.debug("sent tuple:{"+lang+null+tuple.getValueByField("windows").toString());
                }
            }
            mostRecentWindow = tupleWindow;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("lang", "hashtag", "windows"));
    }
}
