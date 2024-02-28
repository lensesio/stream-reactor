package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Provider for BlockingQueuedKafkaConsumers.
 */
@Slf4j
public class KafkaConsumerProvider {
  //Consumer need a unique client ID per thread
  private static int id = 0;

  /**
   * Instantiates BlockingQueuedKafkaConsumer from given properties.
   *
   * @param consumerProperties Properties for Kafka Consumer
   * @param recordBlockingQueue BlockingQueue for ConsumerRecords
   * @return BlockingQueuedKafkaConsumer instance.
   */
  public BlockingQueuedKafkaConsumer createConsumer(Properties consumerProperties,
      BlockingQueue<ConsumerRecords<String, String>> recordBlockingQueue) {
    String clientId;
    synchronized (EventHubsKafkaConsumerController.class) {
      clientId = "KafkaEventHubConsumer#" + id++;
      log.info("Attempting to create Client with Id:{}", clientId);
      consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    }

    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    return new BlockingQueuedKafkaConsumer(recordBlockingQueue, kafkaConsumer, clientId);
  }

}
