package com.datamountaineer.streamreactor.connect.kudu.services;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.CoreUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;
import java.util.Random;

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by
 * default.
 *
 * Requires a running ZooKeeper instance to connect to.  By default, it expects a ZooKeeper instance
 * running at `127.0.0.1:2181`.  You can specify a different ZooKeeper instance by setting the
 * `zookeeper.connect` parameter in the broker's configuration.
 */
public class KafkaEmbedded {

  private static final Logger log = LoggerFactory.getLogger(KafkaEmbedded.class);

  private static final String DEFAULT_ZK_CONNECT = "127.0.0.1:2181";

  private final Properties effectiveConfig;
  private final File logDir;
  private final KafkaServerStartable kafka;

  /**
   * @param config Broker configuration settings.  Used to modify, for example, on which port the
   *               broker should listen to.  Note that you cannot change the `log.dirs` setting
   *               currently.
   */
  public KafkaEmbedded(Properties config) {
    logDir = randomTempDirectory();
    effectiveConfig = effectiveConfigFrom(config);
    KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, true);
    kafka = new KafkaServerStartable(kafkaConfig);
  }

  private File randomTempDirectory() {
    int randomNumber = Math.abs(new Random().nextInt());
    String path =
        System.getProperty("java.io.tmpdir") + File.separator +
        "kafka-embedded-logs-dir-" + randomNumber;
    return new File(path);
  }

  private Properties effectiveConfigFrom(Properties initialConfig) {
    Properties effectiveConfig = new Properties();
    //effectiveConfig.load(this.getClass().getResourceAsStream("/broker-defaults.properties"));
    effectiveConfig.putAll(initialConfig);
    effectiveConfig.setProperty("log.dirs", logDir.getAbsolutePath());
    return effectiveConfig;
  }

  /**
   * This broker's `metadata.broker.list` value.  Example: `127.0.0.1:9092`.
   *
   * You can use this to tell Kafka producers and consumers how to connect to this instance.
   */
  public String brokerList() {
    return kafka.serverConfig().hostName() + ":" + kafka.serverConfig().port().toString();
  }


  /**
   * The ZooKeeper connection string aka `zookeeper.connect`.
   */
  public String zookeeperConnect() {
    return effectiveConfig.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT);
  }

  /**
   * Start the broker.
   */
  public void start() {
    log.debug("Starting embedded Kafka broker at {} (with log.dirs={} and ZK ensemble at {}) ...",
        brokerList(), logDir, zookeeperConnect());
    kafka.startup();
    log.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
        brokerList(), zookeeperConnect());
  }

  /**
   * Stop the broker.
   */
  public void stop() {
    log.debug("Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...",
        brokerList(), zookeeperConnect());
    kafka.shutdown();
    kafka.awaitShutdown();
    log.debug("Removing logs.dir at {} ...", logDir);
    CoreUtils.rm(logDir);
    log.debug("Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
        brokerList(), zookeeperConnect());
  }

  /**
   * Create a Kafka topic with 1 partition and a replication factor of 1.
   *
   * @param topic The name of the topic.
   */
  public void createTopic(String topic) {
    createTopic(topic, 1, 1, new Properties());
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (the partitions of) this topic.
   */
  public void createTopic(String topic, int partitions, int replication) {
    createTopic(topic, partitions, replication, new Properties());
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (partitions of) this topic.
   * @param topicConfig Additional topic-level configuration settings.
   */
  public void createTopic(String topic,
                          int partitions,
                          int replication,
                          Properties topicConfig) {
    log.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
        topic, partitions, replication, topicConfig);
    int sessionTimeoutMs = 10 * 1000;
    int connectionTimeoutMs = 8 * 1000;
    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
    // createTopic() will only seem to work (it will return without error).  The topic will exist in
    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
    // topic.
    ZkClient zkClient = new ZkClient(
        zookeeperConnect(),
        sessionTimeoutMs,
        connectionTimeoutMs,
        ZKStringSerializer$.MODULE$);
    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect()), false);
    AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig);
    zkClient.close();
  }

}