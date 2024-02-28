package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig.getPrefixedKafkaConsumerConfigKey;
import static io.lenses.java.streamreactor.connect.azure.eventhubs.mapping.SourceRecordMapper.mapSourceRecordIncludingHeaders;
import static io.lenses.java.streamreactor.connect.azure.eventhubs.mapping.SourceRecordMapper.mapSourceRecordWithoutHeaders;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfig;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import io.lenses.java.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProvider.AzureOffsetMarker;
import io.lenses.java.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProvider.AzureTopicPartitionKey;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Class is a bridge between EventHub KafkaConsumers and AzureEventHubsSourceTask. It verifies the configuration
 * of kafka consumers and instantiates them, then allows AzureEventHubsSourceTask to pull for SourceRecords.
 */
@Slf4j
public class EventHubsKafkaConsumerController {

  private final AzureEventHubsConfig azureEventHubsConfig;
  private final KafkaConsumerProvider kafkaConsumerProvider;
  private final BlockingQueue<ConsumerRecords<String, String>> recordsQueue;
  private final BlockingQueuedKafkaConsumer queuedKafkaConsumer;

  /**
   * Constructs EventHubsKafkaConsumerController.
   *
   * @param azureEventHubsConfig config for SourceTask
   * @param kafkaConsumerProvider provider of KafkaConsumers
   * @param recordsQueue queue that contains EventHub records.
   */
  public EventHubsKafkaConsumerController(AzureEventHubsConfig azureEventHubsConfig,
      KafkaConsumerProvider kafkaConsumerProvider,
      BlockingQueue<ConsumerRecords<String, String>> recordsQueue) {
    this.azureEventHubsConfig = azureEventHubsConfig;
    this.kafkaConsumerProvider = kafkaConsumerProvider;
    this.recordsQueue = recordsQueue;

    queuedKafkaConsumer = createQueuedConsumer();
  }

  private BlockingQueuedKafkaConsumer createQueuedConsumer() {
    Properties consumerProperties = extractConsumerProperties();
    return kafkaConsumerProvider.createConsumer(consumerProperties, recordsQueue);
  }

  private Properties extractConsumerProperties() {
    Properties consumerProperties = new Properties();

    consumerProperties.put(BOOTSTRAP_SERVERS_CONFIG,
        azureEventHubsConfig.getList(
            getPrefixedKafkaConsumerConfigKey(BOOTSTRAP_SERVERS_CONFIG)));
    consumerProperties.put(GROUP_ID_CONFIG,
        azureEventHubsConfig.getString(
            getPrefixedKafkaConsumerConfigKey(GROUP_ID_CONFIG)));
    consumerProperties.put(SaslConfigs.SASL_JAAS_CONFIG,
        azureEventHubsConfig.getPassword(
            getPrefixedKafkaConsumerConfigKey(SaslConfigs.SASL_JAAS_CONFIG)));
    consumerProperties.put(SaslConfigs.SASL_MECHANISM,
        azureEventHubsConfig.getString(
            getPrefixedKafkaConsumerConfigKey(SaslConfigs.SASL_MECHANISM)));
    consumerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        azureEventHubsConfig.getString(
            getPrefixedKafkaConsumerConfigKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)));
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        azureEventHubsConfig.getClass(
            getPrefixedKafkaConsumerConfigKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)));
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        azureEventHubsConfig.getClass(
            getPrefixedKafkaConsumerConfigKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)));
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    return consumerProperties;
  }

  /**
   * This method leverages BlockingQueue mechanism that BlockingQueuedKafkaConsumer puts EventHub records
   * into. It tries to poll the queue then returns list of SourceRecords
   *
   * @param duration how often to poll.
   * @return list of SourceRecords (can be empty if it couldn't poll from queue)
   * @throws InterruptedException if interrupted while polling
   */
  public List<SourceRecord> poll(Duration duration) throws InterruptedException {
    boolean includeHeaders = azureEventHubsConfig.getBoolean(
        AzureEventHubsConfigConstants.INCLUDE_HEADERS);
    List<SourceRecord> sourceRecords = null;

    queuedKafkaConsumer.startPolling(duration);

    ConsumerRecords<String, String> consumerRecords = null;
    try {
      consumerRecords = recordsQueue.poll(
          duration.get(ChronoUnit.SECONDS), TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.info("{} has been interrupted on poll", this.getClass().getSimpleName());
      throw e;
    }

    if (consumerRecords != null && !consumerRecords.isEmpty()) {
      sourceRecords = new ArrayList<>(consumerRecords.count());
      for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

        AzureTopicPartitionKey azureTopicPartitionKey = new AzureTopicPartitionKey(
            consumerRecord.topic(), consumerRecord.partition());
        AzureOffsetMarker offsetMarker = new AzureOffsetMarker(consumerRecord.offset());

        SourceRecord sourceRecord = includeHeaders
            ? mapSourceRecordIncludingHeaders(consumerRecord, azureTopicPartitionKey,
            offsetMarker, Schema.OPTIONAL_STRING_SCHEMA, Schema.STRING_SCHEMA) :
            mapSourceRecordWithoutHeaders(consumerRecord, azureTopicPartitionKey,
                offsetMarker, Schema.OPTIONAL_STRING_SCHEMA, Schema.STRING_SCHEMA);

        sourceRecords.add(sourceRecord);
      }
    }
    return sourceRecords != null ? sourceRecords : Collections.emptyList();
  }

  void subscribeToTopic(String topic) {
    queuedKafkaConsumer.subscribe(Collections.singletonList(topic));
  }

  public void close(Duration timeoutDuration) {
    queuedKafkaConsumer.close(timeoutDuration);
  }
}
