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
package io.lenses.streamreactor.connect.azure.servicebus.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.azure.servicebus.mapping.SinkRecordToServiceBusMapper;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class AzureServiceBusSinkTaskTest {

  private static final String JAR_MANIFEST_VERSION = "1.2.5";
  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";
  private static final Integer PARTITION1 = 1;
  private static final Long OFFSET10 = 10L;
  private static final Long OFFSET20 = 20L;
  private TaskToSenderBridge taskToSenderBridge;
  private JarManifest jarManifest;

  private AzureServiceBusSinkTask testObj;

  @BeforeEach
  void setUp() {
    taskToSenderBridge = mock(TaskToSenderBridge.class);

    try (MockedConstruction<JarManifest> ignored = Mockito.mockConstruction(JarManifest.class)) {
      testObj = new AzureServiceBusSinkTask();
      jarManifest = ignored.constructed().get(0);
    }

    testObj.initialize(taskToSenderBridge);
  }

  @Test
  void putShouldMapTheRecordAndDelegateToBridge() {
    //given
    ServiceBusMessageWrapper mappedRecord = mock(ServiceBusMessageWrapper.class);
    SinkRecord sinkRecord = mock(SinkRecord.class);
    Set<SinkRecord> recordSet = Set.of(sinkRecord);

    //when
    try (MockedStatic<SinkRecordToServiceBusMapper> mapper = Mockito.mockStatic(SinkRecordToServiceBusMapper.class)) {
      mapper.when(() -> SinkRecordToServiceBusMapper.mapToServiceBus(sinkRecord)).thenReturn(mappedRecord);
      testObj.put(recordSet);
    }

    //then
    verify(taskToSenderBridge).sendMessages(argThat(sbCollection -> sbCollection.size() == 1 && mappedRecord.equals(
        sbCollection.toArray()[0])));
  }

  @Test
  void openShouldInitializeBridgeSenders() {
    //given
    TopicPartition topicPartition = mock(TopicPartition.class);
    Set<TopicPartition> topicPartitionSet = Set.of(topicPartition);

    //when
    testObj.open(topicPartitionSet);

    //then
    verify(taskToSenderBridge).initializeSenders(topicPartitionSet);
  }

  @Test
  void stopShouldCloseSendersOnBridge() {
    //when
    testObj.stop();

    //then
    verify(taskToSenderBridge).closeSenderClients();
  }

  @Test
  void versionShouldReturnJarManifestVersion() {
    //given
    when(jarManifest.getVersion()).thenReturn(JAR_MANIFEST_VERSION);

    //when
    String version = testObj.version();

    //then
    assertEquals(JAR_MANIFEST_VERSION, version);
  }

  @Test
  void preCommitShouldReturnEmptyMapIfTopicPartitionHasNotBeenSeen() {
    //given
    TopicPartition topic1partition1 = new TopicPartition(TOPIC1, PARTITION1);
    TopicPartition topic2partition1 = new TopicPartition(TOPIC2, PARTITION1);

    //when
    Map<TopicPartition, OffsetAndMetadata> result =
        testObj.preCommit(Map.of(
            topic1partition1, new OffsetAndMetadata(OFFSET10),
            topic2partition1, new OffsetAndMetadata(OFFSET10)));

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
        testObj.preCommit(Map.of(
            topic1partition1, new OffsetAndMetadata(OFFSET20),
            topic2partition1, new OffsetAndMetadata(OFFSET20))); //but framework asks for OFFSET20 for both

    //then
    assertEquals(2, result.size());
    assertEquals(OFFSET10, result.get(topic1partition1).offset());
    assertEquals(OFFSET20, result.get(topic2partition1).offset());
  }
}
