/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.common.collections;

import static java.util.Objects.nonNull;

import cyclops.data.tuple.Tuple;
import cyclops.data.tuple.Tuple2;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Storage that holds {@link OffsetAndMetadata} for certain {@link TopicPartition}s.
 */
public class TopicPartitionOffsetAndMetadataStorage {

  private final Map<TopicPartition, OffsetAndMetadata> offsetStorage;

  /**
   * Constructs TopicPartitionOffsetAndMetadataStorage with specific initial capacity.
   */
  public TopicPartitionOffsetAndMetadataStorage() {
    offsetStorage = new ConcurrentHashMap<>();
  }

  /**
   * Constructs TopicPartitionOffsetAndMetadataStorage with specific initial capacity.
   * 
   * @param initialCapacity initial capacity of the storage
   */
  public TopicPartitionOffsetAndMetadataStorage(int initialCapacity) {
    offsetStorage = new ConcurrentHashMap<>(initialCapacity);
  }

  /**
   * Get offset for topic and partition
   * 
   * @param topicPartition {@link TopicPartition} key
   * @return OffsetAndMetadata for the key
   */
  public OffsetAndMetadata get(TopicPartition topicPartition) {
    return offsetStorage.get(topicPartition);
  }

  /**
   * Checks against storage if offsets can be committed.
   * 
   * @param offsetsToCommit offsets that caller want to commit.
   * @return Map of topicPartition keys and offsets values that can be safely committed.
   */
  public Map<TopicPartition, OffsetAndMetadata> checkAgainstProcessedOffsets(
      Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
    return offsetsToCommit.entrySet().stream()
        .map(entry -> Tuple.tuple(entry.getKey(), offsetStorage.get(entry.getKey())))
        .filter(tuple -> nonNull(tuple._2()))
        .collect(Collectors.toUnmodifiableMap(Tuple2::_1, Tuple2::_2));
  }

  /**
   * /**
   * Updates offset using a specific criteria for {@link Map}s merge function. For certain topic+partition it stores
   * either value passed or the one that is already stored (depending on which one is higher)
   *
   * @param topicPartition    {@link TopicPartition} key
   * @param offsetAndMetadata for the key
   * @return value for topicPartition key or null if offset was null
   */
  public OffsetAndMetadata updateOffset(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
    if (offsetAndMetadata != null) {
      OffsetAndMetadata currentlyStoredOffset = offsetStorage.get(topicPartition);
      return offsetStorage.merge(topicPartition, offsetAndMetadata, (key, newOffset) -> newOffset.offset()
          > currentlyStoredOffset.offset() ? newOffset : currentlyStoredOffset);
    }
    return null;
  }

  /**
   * See updateOffset.
   *
   * @param currentOffsets Map of topicPartition keys and offsets values.
   * @return map of topicPartition keys and their offsets
   */
  public Map<TopicPartition, OffsetAndMetadata> updateOffsets(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    return currentOffsets.entrySet().stream()
        .map(entry -> Tuple.tuple(entry.getKey(), updateOffset(entry.getKey(), entry.getValue())))
        .filter(t -> nonNull(t._2()))
        .collect(Collectors.toUnmodifiableMap(Tuple2::_1, Tuple2::_2));
  }

}
