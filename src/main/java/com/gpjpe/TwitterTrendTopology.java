package com.gpjpe;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.gpjpe.bolts.HashtagCountBolt;
import com.gpjpe.bolts.WindowAssignerBolt;
import com.gpjpe.spouts.KafkaTweetsSpout;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SyslogAppender;


public class TwitterTrendTopology {

    private final static Logger LOGGER = Logger.getLogger(TwitterTrendTopology.class.getName());
    private static final String TOPOLOGY_NAME = "Top3";

    public static void main(String[] args) {
        System.out.println("Arguments:");
        for (String arg : args) {
            System.out.print(arg);
        }
        String[] languagesToWatch = args[0].split(",");

        long windowAdvance_s = Long.parseLong((args[2].split(",")[1]));
        long windowSize_s = Long.parseLong((args[2].split(",")[0]));

        if(windowSize_s!=windowAdvance_s){
            throw new IllegalArgumentException("window size and advance must be the same. "
                    +"received: \n window size:" + windowSize_s
                    +"\n window advance:" +windowAdvance_s);
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new KafkaTweetsSpout(languagesToWatch), 1);
        builder.setBolt("windows", new WindowAssignerBolt(windowSize_s),8).shuffleGrouping("spout");
        builder.setBolt("counter", new HashtagCountBolt()).fieldsGrouping("windows", new Fields("lang"));
//        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
//        builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

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
