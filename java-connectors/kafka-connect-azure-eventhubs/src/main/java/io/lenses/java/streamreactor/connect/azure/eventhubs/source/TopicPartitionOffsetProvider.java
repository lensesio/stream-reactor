package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 * This class represents an abstraction over OffsetStorageReader that can be freely called by Azure
 * EventHub Kafka Consumers when it was initialized once. It helps consumers to find out which
 * offset were already committed inside Kafka Connect.
 */
@Slf4j
public final class TopicPartitionOffsetProvider {

  private static final String OFFSET_KEY = "OFFSET";

  private static OffsetStorageReader offsetStorageReader;

  private static TopicPartitionOffsetProvider instance;

  /**
   * This method is to be called once to initialize the singleton instance of
   * TopicPartitionOffsetProvider.
   *
   * @param offsetStorageReader instance of OffsetStorageReader from context.
   */
  public static synchronized void initialize(OffsetStorageReader offsetStorageReader) {
    if (instance == null) {
      if (offsetStorageReader != null) {
        TopicPartitionOffsetProvider.offsetStorageReader = offsetStorageReader;
      }
      TopicPartitionOffsetProvider.instance = new TopicPartitionOffsetProvider();
    } else {
      log.warn("Tried to initialize {} with null {}!",
          TopicPartitionOffsetProvider.class.getSimpleName(),
          OffsetStorageReader.class.getSimpleName());
    }
  }

  private TopicPartitionOffsetProvider() {
  }

  /**
   * This method is to return instance of class. Returns Optional with instance or empty if not
   * initialized.
   */
  public static synchronized Optional<TopicPartitionOffsetProvider> getInstance() {
    return Optional.ofNullable(instance);
  }

  /**
   * Checks for committed offsets for topic+partition combo.
   *
   * @param azureTopicPartitionKey key of topic+partition combo.
   *
   * @return empty optional if topic+partition combo has not committed any offsets or
   *     AzureOffsetMarker if combo already did commit some.
   */
  public Optional<AzureOffsetMarker> getOffset(AzureTopicPartitionKey azureTopicPartitionKey) {
    Optional<AzureOffsetMarker> toReturn = Optional.empty();
    Optional<Map<String, Object>> offsetOptional = Optional.ofNullable(
        offsetStorageReader.offset(azureTopicPartitionKey));

    if (offsetOptional.isPresent()) {
      Long offset = (Long) offsetOptional.get().get(OFFSET_KEY);
      if (offset != null) {
        toReturn = Optional.of(new AzureOffsetMarker(offset));
      }
    }

    return toReturn;
  }

  /**
   * This class represents immutable map that represents topic and partition combo used by
   * TopicPartitionOffsetProvider.
   */
  public static class AzureTopicPartitionKey extends HashMap<String, String> {

    private static final String TOPIC_KEY = "TOPIC";
    private static final String PARTITION_KEY = "PARTITION";

    public AzureTopicPartitionKey(String topic, Integer partition) {
      this.put(TOPIC_KEY, topic);
      this.put(PARTITION_KEY, partition.toString());
    }

    public String getTopic() {
      return get(TOPIC_KEY);
    }

    public Integer getPartition() {
      return Integer.valueOf(get(PARTITION_KEY));
    }
  }

  /**
   * This class represents immutable map that represents topic and partition combo offset used by
   * Kafka Connect SourceRecords.
   */
  public static class AzureOffsetMarker extends HashMap<String, Object> {

    public AzureOffsetMarker(Long offset) {
      put(OFFSET_KEY, offset);
    }

    public Long getOffsetValue() {
      return (Long) get(OFFSET_KEY);
    }
  }
}
