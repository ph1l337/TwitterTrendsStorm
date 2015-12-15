package com.gpjpe;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.gpjpe.bolts.HashtagCountBolt;
import com.gpjpe.bolts.NewWindowNotifierBolt;
import com.gpjpe.bolts.WindowAssignerBolt;
import com.gpjpe.helpers.Utils;
import com.gpjpe.spouts.KafkaTweetsSpout;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class TwitterTrendTopology {

    private final static Logger LOGGER = Logger.getLogger(TwitterTrendTopology.class.getName());

    public static void validateParameters(String[] params) {

        if (params == null || params.length < 5) {
            throw new RuntimeException("Expected 5 arguments: " +
                    "langList zookeeperURI winParams topologyName dataFolder");
        }

        List<String> messages = new ArrayList<>();
        if (!params[1].contains(":")) {
            messages.add(
                    String.format("Expected Zookeeper URI format is [IP:PORT], got %s", params[1]));
        }

        if (!params[2].contains(",")) {
            messages.add(
                    String.format("Expected Window Configuration format is [Size,Slide], got %s", params[1]));
        }

        if (!messages.isEmpty()) {
            for (String message : messages) {
                LOGGER.error(message);
            }
            throw new IllegalArgumentException("Check parameters format");
        }

        LOGGER.info(String.format("Languages: %s", params[0]));
        LOGGER.info(String.format("ZookeeperURI: %s", params[1]));
        LOGGER.info(String.format("WindowConfig: %s", params[2]));
        LOGGER.info(String.format("TopologyName: %s", params[3]));
        LOGGER.info(String.format("DataFolder: %s", params[4]));
    }

    public static void main(String[] args) {

        validateParameters(args);

        String[] languagesToWatch = args[0].split(",");
        String zookeeperURI = args[1];
        String[] windowConfig = args[2].split(",");
        String topologyName = args[3];
        String storagePath = args[4];

        long windowSizeSeconds = Long.parseLong(windowConfig[0]);
        long windowAdvanceSeconds = Long.parseLong(windowConfig[1]);

        if (windowSizeSeconds < windowAdvanceSeconds) {
            throw new IllegalArgumentException("Window size and advance greater than the advance. "
                    + "\nWindow size:" + windowSizeSeconds
                    + "\nWindow advance:" + windowAdvanceSeconds);
        }

        TopologyBuilder builder = new TopologyBuilder();
        AppConfig appConfig = new AppConfig();
        String topic = appConfig.getProperty(CONFIG.KAFKA_TOPIC, "TweetStream");
        int maxWindows = Utils.calcMaxAmountofWindows(windowSizeSeconds, windowAdvanceSeconds);
        String logSuffix = "05";

        //TODO: DELETE FILES FOR EACH LANG BEFORE STARTING

        //TODO: set parallelism for more: threads == tasks
        builder.setSpout("spout", new KafkaTweetsSpout(languagesToWatch, zookeeperURI, topic), 1);
        builder.setBolt("windows", new WindowAssignerBolt(windowSizeSeconds, windowAdvanceSeconds), 8).shuffleGrouping("spout");
        builder.setBolt("newWindowNotifier", new NewWindowNotifierBolt(languagesToWatch), 8).shuffleGrouping("windows");
        builder.setBolt("counter", new HashtagCountBolt(3, maxWindows, storagePath, logSuffix))
                .fieldsGrouping("newWindowNotifier", new Fields("lang"))
                .setNumTasks(languagesToWatch.length);

        Config conf = new Config();
        conf.setNumWorkers(3);
        conf.setDebug(true);

        //TODO: submit to running storm topology
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, builder.createTopology());

        //TODO: remove for deployment
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            LOGGER.error(e);
        }

        cluster.shutdown();
    }
}
