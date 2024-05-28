/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.azure.servicebus.source;

import java.util.HashMap;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.storage.OffsetStorageReader;

/**
 * This class represents an abstraction over OffsetStorageReader that can be freely called by Azure
 * ServiceBus Consumers when it was initialized once. It helps consumers to find out which
 * messageIds were already committed inside Kafka Connect.
 */
@Slf4j
public final class ServiceBusPartitionOffsetProvider {

  private static final String MESSAGE_ID_KEY = "MESSAGE_ID";

  private final OffsetStorageReader offsetStorageReader;

  public ServiceBusPartitionOffsetProvider(OffsetStorageReader offsetStorageReader) {
    this.offsetStorageReader = offsetStorageReader;
  }

  /**
   * Checks for committed offsets for topic+partition combo.
   *
   * @param azureServiceBusPartitionKey key of topic+partition combo.
   *
   * @return empty optional if topic+partition combo has not committed any offsets or
   *         AzureOffsetMarker if combo already did commit some.
   */
  public Optional<AzureServiceBusOffsetMarker> getOffset(AzureServiceBusPartitionKey azureServiceBusPartitionKey) {
    return Optional.ofNullable(offsetStorageReader.offset(azureServiceBusPartitionKey))
        .map(offsetMap -> (Long) offsetMap.get(MESSAGE_ID_KEY))
        .map(AzureServiceBusOffsetMarker::new);
  }

  /**
   * This class represents immutable map that represents bus and partition combo used by
   * TopicPartitionOffsetProvider.
   */
  @EqualsAndHashCode(callSuper = true)
  public static class AzureServiceBusPartitionKey extends HashMap<String, String> {

    private static final String BUS_KEY = "BUS";
    private static final String PARTITION_KEY = "PARTITION";

    public AzureServiceBusPartitionKey(String topic, String partition) {
      this.put(BUS_KEY, topic);
      this.put(PARTITION_KEY, partition);
    }
  }

  /**
   * This class represents immutable map that represents bus and partition combo offset used by
   * Kafka Connect SourceRecords.
   */
  public static class AzureServiceBusOffsetMarker extends HashMap<String, Object> {

    public AzureServiceBusOffsetMarker(Long offset) {
      put(MESSAGE_ID_KEY, offset);
    }

    public Long getOffsetValue() {
      return (Long) get(MESSAGE_ID_KEY);
    }
  }
}
