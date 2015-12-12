package com.gpjpe;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.gpjpe.spouts.KafkaTweetsSpout;
import org.apache.log4j.Logger;


public class TwitterTrendTopology {

    private final static Logger LOGGER = Logger.getLogger(TwitterTrendTopology.class.getName());
    private static final String TOPOLOGY_NAME = "Top3";

    public static void main(String[] args){

        String[] languagesToWatch = args[0].split(",");
//        String zookeeperURI = args[1];

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new KafkaTweetsSpout(languagesToWatch), 1);
//        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
//        builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));
//
        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());

        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            LOGGER.error(e);
        }

        cluster.shutdown();
    }
}
