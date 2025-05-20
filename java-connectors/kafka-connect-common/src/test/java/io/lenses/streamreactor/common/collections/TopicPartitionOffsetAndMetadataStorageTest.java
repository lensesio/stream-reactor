/*
 * Copyright 2017-2025 Lenses.io Ltd
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TopicPartitionOffsetAndMetadataStorageTest {

  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";
  private static final Integer PARTITION1 = 1;
  private static final Long OFFSET10 = 10L;
  private static final Long OFFSET20 = 20L;

  TopicPartitionOffsetAndMetadataStorage testObj;

  @BeforeEach
  void setUp() {
    testObj = new TopicPartitionOffsetAndMetadataStorage();
  }

  @Test
  void getShouldReturnNullIfTopicPartitionHasNotBeenUpdatedBefore() {

    //when
    OffsetAndMetadata offsetAndMetadata = testObj.get(new TopicPartition("unknownTopic", 1));

    //then
    assertNull(offsetAndMetadata);
  }

  @Test
  void getShouldReturnCommitAndMetadataIfTopicPartitionHasBeenUpdatedBefore() {
    //given
    TopicPartition knownTopicPartition = new TopicPartition(TOPIC1, PARTITION1);
    OffsetAndMetadata knownOffset = new OffsetAndMetadata(OFFSET10);
    testObj.updateOffset(knownTopicPartition, knownOffset);

    //when
    OffsetAndMetadata offsetAndMetadata = testObj.get(knownTopicPartition);

    //then
    assertNotNull(offsetAndMetadata);
    assertEquals(knownOffset, offsetAndMetadata);
  }

  @Test
  void preCommitShouldReturnEmptyMapIfOffsetPassedAsNull() {
    //given
    TopicPartition topic1partition1 = new TopicPartition(TOPIC1, PARTITION1);
    TopicPartition topic2partition1 = new TopicPartition(TOPIC2, PARTITION1);

    HashMap<TopicPartition, OffsetAndMetadata> nullOffsetMap = new HashMap<>();
    nullOffsetMap.put(topic1partition1, null);
    nullOffsetMap.put(topic2partition1, null);

    //when
    Map<TopicPartition, OffsetAndMetadata> result =
        testObj.updateOffsets(nullOffsetMap);

    //then
    assertEquals(0, result.size());
  }

  @Test
  void preCommitShouldReturnHighestOffsetForTopicPartitionAtTheTimeOfCall() {
    //given
    TopicPartition topic1partition1 = new TopicPartition(TOPIC1, PARTITION1);
    TopicPartition topic2partition1 = new TopicPartition(TOPIC2, PARTITION1);

    testObj.updateOffsets(Map.of(
        topic1partition1, new OffsetAndMetadata(OFFSET10),
        topic2partition1, new OffsetAndMetadata(OFFSET20))); //only topic2part2 has committed OFFSET20 already

    //when
    Map<TopicPartition, OffsetAndMetadata> result =
        testObj.checkAgainstProcessedOffsets(Map.of(
            topic1partition1, new OffsetAndMetadata(OFFSET20),
            topic2partition1, new OffsetAndMetadata(OFFSET20))); //but framework asks for OFFSET20 for both

    //then
    assertEquals(2, result.size());
    assertEquals(OFFSET10, result.get(topic1partition1).offset());
    assertEquals(OFFSET20, result.get(topic2partition1).offset());
  }
}
