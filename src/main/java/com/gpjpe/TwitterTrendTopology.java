package com.gpjpe;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.gpjpe.bolts.HashtagCountBolt;
import com.gpjpe.bolts.NewWindowNotifierBolt;
import com.gpjpe.bolts.WindowAssignerBolt;
import com.gpjpe.helpers.Utils;
import com.gpjpe.spouts.FakeTweetsSpout;
import com.gpjpe.spouts.KafkaTweetsSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class TwitterTrendTopology {

    private final static Logger LOGGER = LoggerFactory.getLogger(TwitterTrendTopology.class.getName());

    public static void validateParameters(String[] params) {

        if (params == null || params.length < 9) {
            throw new RuntimeException("Expected 8 arguments: " +
                    "langList zookeeperURI winParams topologyName dataFolder logSuffix mode source debug");
        }

        List<String> messages = new ArrayList<String>();
        if (!params[1].contains(":")) {
            messages.add(
                    String.format("Expected Zookeeper URI format is [IP:PORT], got %s", params[1]));
        }

        if (!params[2].contains(",")) {
            messages.add(
                    String.format("Expected Window Configuration format is [Size,Slide], got %s", params[2]));
        }

        if (!TopologyRunMode.modes().contains(params[6].toUpperCase())) {
            messages.add(
                    String.format("Mode should be either `local` or `remote`: %s", params[6])
            );
        }

        if (!TopologyDataSource.modes().contains(params[7].toUpperCase())) {
            messages.add(
                    String.format("Source should be either `internal` or `kafka`: %s", params[7])
            );
        }

        if (!Arrays.asList(new String[]{"true", "false"}).contains(params[8].toLowerCase())) {
            messages.add(
                    String.format("Debug should be either `true` or `false`: %s", params[8])
            );
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
        LOGGER.info(String.format("LogSuffix: %s", params[5]));
        LOGGER.info(String.format("Mode: %s", params[6]));
        LOGGER.info(String.format("Source: %s", params[7]));
        LOGGER.info(String.format("Debug: %s", params[8]));
    }

    public static void clearFiles(String[] languages, String outputDir){

        File file;

        for(String language: languages){
            file = new File(String.format("%s/%s", outputDir, language));

            if (file.exists()){

                LOGGER.info(
                        String.format(
                                "Deleting file %s: %s", file.getPath(), file.delete()
                        )
                );
            }
        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        validateParameters(args);

        String[] languagesToWatch = args[0].split(",");
        String zookeeperURI = args[1];
        String[] windowConfig = args[2].split(",");
        String topologyName = args[3];
        String storagePath = args[4];
        String logSuffix = args[5];
        TopologyRunMode mode = TopologyRunMode.valueOf(args[6].toUpperCase());
        TopologyDataSource source = TopologyDataSource.valueOf(args[7].toUpperCase());
        boolean debug = Boolean.valueOf(args[8].toUpperCase());

        long windowSizeSeconds = Long.parseLong(windowConfig[0]);
        long windowAdvanceSeconds = Long.parseLong(windowConfig[1]);

        if (windowSizeSeconds < windowAdvanceSeconds) {
            throw new IllegalArgumentException("Window size and advance greater than the advance. "
                    + "\nWindow size:" + windowSizeSeconds
                    + "\nWindow advance:" + windowAdvanceSeconds);
        }

        clearFiles(languagesToWatch, storagePath);

        TopologyBuilder builder = new TopologyBuilder();
        AppConfig appConfig = new AppConfig();
        String topic = appConfig.getProperty(ConfigParams.KAFKA_TOPIC, "TweetStream");
        int maxWindows = Utils.calcMaxAmountofWindows(windowSizeSeconds, windowAdvanceSeconds);

        if (source == TopologyDataSource.INTERNAL) {
            builder.setSpout("spout", new FakeTweetsSpout(languagesToWatch), 1);
        } else {
            builder.setSpout("spout", new KafkaTweetsSpout(languagesToWatch, zookeeperURI, topic), 1);
        }

        builder.setBolt("windows", new WindowAssignerBolt(windowSizeSeconds, windowAdvanceSeconds), 1)
                .shuffleGrouping("spout");
//        builder.setBolt("newWindowNotifier", new NewWindowNotifierBolt(languagesToWatch, maxWindows), 1).shuffleGrouping("windows");
        builder.setBolt("counter", new HashtagCountBolt(3, maxWindows, windowAdvanceSeconds, storagePath, logSuffix),
                languagesToWatch.length)
                .fieldsGrouping("windows", new Fields("lang"))
                .setNumTasks(languagesToWatch.length);

        backtype.storm.Config conf = new backtype.storm.Config();
        conf.setNumWorkers(4);
        conf.setDebug(debug);

        if (mode == TopologyRunMode.LOCAL) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());

            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                LOGGER.error(e.toString());
            }

            cluster.shutdown();
        } else {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        }
    }
}
