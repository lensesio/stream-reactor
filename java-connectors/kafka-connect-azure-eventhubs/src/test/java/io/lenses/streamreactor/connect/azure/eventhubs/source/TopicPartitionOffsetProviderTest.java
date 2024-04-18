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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lenses.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProvider.AzureOffsetMarker;
import io.lenses.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProvider.AzureTopicPartitionKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

class TopicPartitionOffsetProviderTest {

  @Test
    void getOffsetShouldCallOffsetStorageReader() {
      //given
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      TopicPartitionOffsetProvider topicPartitionOffsetProvider = new TopicPartitionOffsetProvider(
          offsetStorageReader);
      String topic = "some_topic";
      Integer partition = 1;

      //when
      AzureTopicPartitionKey azureTopicPartitionKey = new AzureTopicPartitionKey(topic, partition);
      topicPartitionOffsetProvider.getOffset(azureTopicPartitionKey);

      //then
      verify(offsetStorageReader).offset(azureTopicPartitionKey);
    }

    @Test
    void getOffsetShouldReturnEmptyOptionalIfCommitsNotFound() {
      //given
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      when(offsetStorageReader.offset(any(Map.class))).thenReturn(new HashMap());
      String topic = "some_topic";
      Integer partition = 1;
      TopicPartitionOffsetProvider topicPartitionOffsetProvider = new TopicPartitionOffsetProvider(
          offsetStorageReader);

      //when
      AzureTopicPartitionKey azureTopicPartitionKey = new AzureTopicPartitionKey(topic, partition);
      Optional<AzureOffsetMarker> offset = topicPartitionOffsetProvider.getOffset(azureTopicPartitionKey);

      //then
      verify(offsetStorageReader).offset(azureTopicPartitionKey);
      assertTrue(offset.isEmpty());
    }

    @Test
    void getOffsetShouldReturnValidAzureOffsetMarkerIfCommitsFound() {
      //given
      long offsetOne = 1L;
      String OFFSET_KEY = "OFFSET";
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      HashMap<String, Long> offsets = new HashMap<>();
      offsets.put(OFFSET_KEY, offsetOne);
      when(offsetStorageReader.offset(any(Map.class))).thenReturn(offsets);
      String topic = "some_topic";
      Integer partition = 1;
      TopicPartitionOffsetProvider topicPartitionOffsetProvider = new TopicPartitionOffsetProvider(
          offsetStorageReader);

      //when
      AzureTopicPartitionKey azureTopicPartitionKey = new AzureTopicPartitionKey(topic, partition);
      Optional<AzureOffsetMarker> offset = topicPartitionOffsetProvider.getOffset(azureTopicPartitionKey);

      //then
      verify(offsetStorageReader).offset(azureTopicPartitionKey);
      assertTrue(offset.isPresent());
      assertEquals(offsetOne, offset.get().getOffsetValue());
    }

    @Test
    void AzureTopicPartitionKeyShouldReturnTopicAndPartitionValues() {
      //given
      int partition = 10;
      String topic = "topic";

      //when
      final AzureTopicPartitionKey azureTopicPartitionKey = new AzureTopicPartitionKey(topic, partition);

      //then
      assertEquals(partition, azureTopicPartitionKey.getPartition());
      assertEquals(topic, azureTopicPartitionKey.getTopic());
    }

}