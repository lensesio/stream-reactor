package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig.getPrefixedKafkaConsumerConfigKey;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Provider for BlockingQueuedKafkaConsumers.
 */
@Slf4j
public class BlockingQueueProducerProvider implements ProducerProvider {

  private final TopicPartitionOffsetProvider topicPartitionOffsetProvider;
  private static final boolean STRIP_PREFIX = true;

  public BlockingQueueProducerProvider(TopicPartitionOffsetProvider topicPartitionOffsetProvider) {
    this.topicPartitionOffsetProvider = topicPartitionOffsetProvider;
  }

  /**
   * Instantiates BlockingQueuedKafkaConsumer from given properties.
   *
   * @param azureEventHubsConfig Config of Task
   * @param recordBlockingQueue BlockingQueue for ConsumerRecords
   * @return BlockingQueuedKafkaConsumer instance.
   */
  public BlockingQueuedKafkaProducer createProducer(AzureEventHubsConfig azureEventHubsConfig,
      BlockingQueue<ConsumerRecords<String, String>> recordBlockingQueue) {
    String topic = azureEventHubsConfig.getString(AzureEventHubsConfigConstants.EVENTHUB_NAME);
    String connectorName = azureEventHubsConfig.getString(AzureEventHubsConfigConstants.CONNECTOR_NAME);
    final String clientId = connectorName + "#" + UUID.randomUUID();
    log.info("Attempting to create Client with Id:{}", clientId);

    Map<String, Object> consumerProperties = azureEventHubsConfig.originalsWithPrefix(
        AzureEventHubsConfigConstants.CONNECTOR_WITH_CONSUMER_PREFIX, STRIP_PREFIX);

    consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, azureEventHubsConfig.getString(
            getPrefixedKafkaConsumerConfigKey(GROUP_ID_CONFIG)));
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        azureEventHubsConfig.getClass(
            getPrefixedKafkaConsumerConfigKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)));
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        azureEventHubsConfig.getClass(
            getPrefixedKafkaConsumerConfigKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)));
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);

    return new BlockingQueuedKafkaProducer(topicPartitionOffsetProvider, recordBlockingQueue, kafkaConsumer, clientId, topic);
  }

}
