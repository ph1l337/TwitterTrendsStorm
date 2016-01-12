package com.gpjpe.domain.reader;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;


public class KafkaStreamReader implements IStreamReader {

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaStreamReader.class.getName());

    private ConsumerConnector consumer;
    private ExecutorService executor;
    private BlockingQueue<String> queue;
    private String topic;

    public KafkaStreamReader(String zookeeperURI, String groupId, String topic) {
        init(zookeeperURI, groupId, topic);
    }

    private void init(String zookeeperURI, String groupId, String topic) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zookeeperURI);
        properties.put("group.id", groupId);
        properties.put("zookeeper.session.timeout", "400");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");

        ConsumerConfig consumerConfig = new ConsumerConfig(properties);

        int queueSize = 1000;
        this.queue = new ArrayBlockingQueue<String>(queueSize, true);
        this.topic = topic;
        this.consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            assert executor != null;
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                LOGGER.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void run(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, numThreads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(numThreads);

        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new BasicKafkaReader(stream, threadNumber, this.queue));
            threadNumber++;
        }
    }

    @Override
    public String nextTweet() {

        String message = null;

        try {
            message = queue.take();
        } catch (InterruptedException e) {
            LOGGER.error(e.toString());
        }

        return message;
    }

    private class BasicKafkaReader implements Runnable {
        private KafkaStream stream;
        private int threadNumber;
        private BlockingQueue<String> queue;

        public BasicKafkaReader(KafkaStream stream, int threadNumber, BlockingQueue<String> queue) {
            this.threadNumber = threadNumber;
            this.stream = stream;
            this.queue = queue;
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            String message;
            while (it.hasNext()) {
                try {
                    message = new String(it.next().message(),"UTF-8");
                    queue.put(message);
                    LOGGER.debug("Thread " + threadNumber + ": " + message);
                } catch (InterruptedException e) {
                    LOGGER.error(e.toString());
                } catch (UnsupportedEncodingException e) {
                    LOGGER.error(e.toString());
                }
            }
            LOGGER.debug("Shutting down Thread: " + threadNumber);
        }
    }
}
