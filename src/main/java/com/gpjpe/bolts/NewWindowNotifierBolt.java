package com.gpjpe.bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

import com.gpjpe.helpers.Utils;
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

        String tupleLanguage = (String) tuple.getValueByField("lang");
        String tutpleHashTag = (String) tuple.getValueByField("hashtag");
        Long[] windows = (Long[]) (tuple.getValueByField("windows"));

        Long tupleTopWindow = windows[0];

        //always pass on the tuple received.
        _collector.emit(new Values(
                        tupleLanguage,
                        tutpleHashTag,
                        windows)
        );

        LOGGER.debug(
                String.format(
                        "Sent tuple: {%s, %s, %s}",
                        tupleLanguage,
                        tutpleHashTag,
                        Utils.Stringify(windows, ",")
                )
        );


        if (mostRecentWindow == null) {
            mostRecentWindow = tupleTopWindow;
        }

        //if a with a new most recent window arrives sent out tuples with hashtag=null to every language in question

        if (!mostRecentWindow.equals(tupleTopWindow)) {
            for (String lang : langs) {
                if (!lang.equals(tupleLanguage)) {
                    _collector.emit(new Values(
                            lang,
                            null,
                            windows));

                    LOGGER.info(
                            String.format(
                                    "Sent tuple:{%s, %s, %s}, to flush earliest Window",
                                    lang, null, Utils.Stringify(windows, ",")
                            )
                    );
                }
            }
            mostRecentWindow = tupleTopWindow;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("lang", "hashtag", "windows"));
    }
}
