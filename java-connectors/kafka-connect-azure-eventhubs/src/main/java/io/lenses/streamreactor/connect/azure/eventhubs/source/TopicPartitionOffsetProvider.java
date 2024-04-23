/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.azure.eventhubs.source;

import java.util.HashMap;
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

  private final OffsetStorageReader offsetStorageReader;


  public TopicPartitionOffsetProvider(OffsetStorageReader offsetStorageReader) {
    this.offsetStorageReader = offsetStorageReader;
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
    return Optional.ofNullable(offsetStorageReader.offset(azureTopicPartitionKey))
        .map(offsetMap -> (Long) offsetMap.get(OFFSET_KEY))
        .map(AzureOffsetMarker::new);
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
