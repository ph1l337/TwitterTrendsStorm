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

public class NewWindowNotifierBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewWindowNotifierBolt.class.getName());
    private OutputCollector _collector;
    private String[] languages;
    private int maxWindows;
    private Long[] currentWindows;

    public NewWindowNotifierBolt(String[] languages, int maxWindows) {
        this.languages = languages;
        this.maxWindows = maxWindows;
        this.currentWindows = new Long[maxWindows];
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String tupleLanguage = (String) tuple.getValueByField("lang");
        String tupleHashTag = (String) tuple.getValueByField("hashtag");
        Long[] tupleWindows = (Long[]) (tuple.getValueByField("windows"));

        if (this.currentWindows[0] == null) {
            System.arraycopy(tupleWindows, 0, this.currentWindows, 0, tupleWindows.length);
        }

        if (this.currentWindows[0].compareTo(tupleWindows[0]) != 0) {

            if (this.currentWindows[maxWindows - 1] != null) {

                Long windowToFlush = this.currentWindows[maxWindows - 1];

                LOGGER.info(
                        String.format("Front most window changed to [%s], flushing last earliest window [%s]",
                                tupleWindows[0],
                                windowToFlush
                        )
                );

                for (String lang : languages) {
                    _collector.emit(new Values(
                            lang,
                            null,
                            null,
                            windowToFlush));

                    LOGGER.info(
                            String.format(
                                    "Sent tuple:{%s, %s, %s, %s}, to flush earliest Window",
                                    lang, null, Utils.Stringify(tupleWindows, ","), windowToFlush
                            )
                    );
                }
            }

            //update current windows
            System.arraycopy(tupleWindows, 0, this.currentWindows, 0, tupleWindows.length);
        }

        this._collector.emit(
                new Values(
                        tupleLanguage,
                        tupleHashTag,
                        tupleWindows,
                        null)
        );

        this._collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("lang", "hashtag", "windows", "windowToFlush"));
    }
}
