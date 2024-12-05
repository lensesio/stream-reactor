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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.azure.messaging.servicebus.ServiceBusErrorSource;
import com.azure.messaging.servicebus.ServiceBusException;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusConfigConstants;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusSinkConfig;
import io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusSinkMapping;
import io.lenses.streamreactor.connect.azure.servicebus.util.ServiceBusKcqlProperties;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskToSenderBridgeTest {

  private static final String CONNECTION_STRING =
      "Endpoint=sb://TESTENDPOINT.servicebus.windows.net/;"
          + "SharedAccessKeyName=EXAMPLE_NAME;SharedAccessKey=EXAMPLE_KEY";
  private static final int MAX_RETRIES = 3;
  private static final int RETRY_TIMEOUT = 10;
  private static final String KAFKA_TOPIC = "ktopic";
  private static final String KAFKA_TOPIC2 = "ktopic2";
  private static final String SBUS_NAME = "servicebus";
  private static final String MAPPING_NOT_FOUND_EXCEPTION_MSG =
      "Configuration Exception:Necessary KCQL Mapping for topic ktopic is not found. Connector is exiting.";

  private static final String SENDER_EXCEPTION_MSG = "Number of retries exhausted. Cause:";
  private static final Consumer<Map<TopicPartition, OffsetAndMetadata>> COMMIT_FUNCTION = mock(Consumer.class);
  private static final AzureServiceBusSinkConfig SINK_CONFIG = mock(AzureServiceBusSinkConfig.class);
  private static final String QUEUE_TYPE = "QUEUE";
  private static ServiceBusSenderFacade senderFacade = mock(ServiceBusSenderFacade.class);
  private static ServiceBusSenderFacade senderFacade2 = mock(ServiceBusSenderFacade.class);
  private static ServiceBusSinkMapping sinkMapping = mock(ServiceBusSinkMapping.class);

  private TaskToSenderBridge testObj;

  @BeforeEach
  void setUp() {
    sinkMapping = mock(ServiceBusSinkMapping.class);
    senderFacade2 = mock(ServiceBusSenderFacade.class);
    senderFacade = mock(ServiceBusSenderFacade.class);

    when(SINK_CONFIG.getInt(AzureServiceBusConfigConstants.MAX_NUMBER_OF_RETRIES)).thenReturn(MAX_RETRIES);
    when(SINK_CONFIG.getInt(AzureServiceBusConfigConstants.TIMEOUT_BETWEEN_RETRIES)).thenReturn(RETRY_TIMEOUT);
    when(SINK_CONFIG.getString(AzureServiceBusConfigConstants.CONNECTION_STRING)).thenReturn(CONNECTION_STRING);

    testObj =
        new TaskToSenderBridge(SINK_CONFIG, Map.of(KAFKA_TOPIC, senderFacade),
            COMMIT_FUNCTION, Map.of(KAFKA_TOPIC, sinkMapping));
  }

  @Test
  void initializeSendersShouldInitializeIfSenderNotYetInitialized() {
    //given
    int partitionAssigned = 3;

    Map<String, ServiceBusSenderFacade> sendersMapMock = mock(Map.class);
    //first return null (not found) then the one we instantiate
    when(sendersMapMock.get(KAFKA_TOPIC)).thenReturn(null, senderFacade);

    TopicPartition topicPartition = mock(TopicPartition.class);
    when(topicPartition.topic()).thenReturn(KAFKA_TOPIC);
    when(topicPartition.partition()).thenReturn(partitionAssigned);

    when(sinkMapping.getInputKafkaTopic()).thenReturn(KAFKA_TOPIC);
    when(sinkMapping.getOutputServiceBusName()).thenReturn(SBUS_NAME);
    when(sinkMapping.getProperties())
        .thenReturn(Map.of(ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName(), QUEUE_TYPE));

    //when
    testObj =
        new TaskToSenderBridge(SINK_CONFIG, sendersMapMock,
            COMMIT_FUNCTION, Map.of(KAFKA_TOPIC, sinkMapping));

    Optional<ConfigException> configException = testObj.initializeSenders(Set.of(topicPartition));

    //then
    assertTrue(configException.isEmpty());
    verify(topicPartition, times(4)).topic();
    verify(sinkMapping, times(2)).getInputKafkaTopic();
    verify(sinkMapping, times(2)).getProperties();
    verify(sinkMapping).getOutputServiceBusName();
    verify(senderFacade).initializePartition(topicPartition);
  }

  @Test
  void initializeSendersShouldErrorIfNoMappingFound() {
    //given

    Map<String, ServiceBusSinkMapping> mappingsMapMock = mock(Map.class);
    when(mappingsMapMock.get(KAFKA_TOPIC)).thenReturn(null);

    Map<String, ServiceBusSenderFacade> sendersMapMock = mock(Map.class);
    when(sendersMapMock.get(KAFKA_TOPIC)).thenReturn(null);

    TopicPartition topicPartition = mock(TopicPartition.class);
    when(topicPartition.topic()).thenReturn(KAFKA_TOPIC);

    when(sinkMapping.getInputKafkaTopic()).thenReturn(KAFKA_TOPIC);
    when(sinkMapping.getOutputServiceBusName()).thenReturn(SBUS_NAME);
    when(sinkMapping.getProperties())
        .thenReturn(Map.of(ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName(), QUEUE_TYPE));

    //when
    testObj =
        new TaskToSenderBridge(SINK_CONFIG, sendersMapMock,
            COMMIT_FUNCTION, mappingsMapMock);

    Optional<ConfigException> configException = testObj.initializeSenders(Set.of(topicPartition));

    //then
    assertFalse(configException.isEmpty());
    assertEquals(MAPPING_NOT_FOUND_EXCEPTION_MSG, configException.get().getMessage());
    verify(topicPartition, times(1)).topic();
    verifyNoInteractions(sinkMapping);
    verifyNoInteractions(senderFacade);
  }

  @Test
  void sendMessagesShouldDelegateSendingToSenderForTheTopic() {
    //given
    ServiceBusMessageWrapper serviceBusRecord =
        mockServiceBusRecordComposite(KAFKA_TOPIC);

    Set<ServiceBusMessageWrapper> serviceBusRecords = Set.of(serviceBusRecord);

    when(senderFacade.sendMessages(anyCollection())).thenReturn(Optional.empty());

    //when
    testObj.sendMessages(serviceBusRecords);

    //then
    verify(serviceBusRecord).getOriginalTopic();
    verify(senderFacade).sendMessages(argThat(messagesCollection -> messagesCollection.contains(serviceBusRecord)));
  }

  @Test
  void sendMessagesShouldReturnExceptionListIfSenderReturnsOneAfterRetries() {
    //given
    ServiceBusMessageWrapper serviceBusRecord =
        mockServiceBusRecordComposite(KAFKA_TOPIC);
    ServiceBusException busException =
        new ServiceBusException(new RuntimeException("Bad Times"), ServiceBusErrorSource.UNKNOWN);

    Set<ServiceBusMessageWrapper> serviceBusRecords = Set.of(serviceBusRecord);

    when(senderFacade.sendMessages(anyCollection())).thenReturn(Optional.of(busException));

    //when
    List<ServiceBusSendingException> serviceBusSendingExceptions = testObj.sendMessages(serviceBusRecords);

    //then
    assertFalse(serviceBusSendingExceptions.isEmpty());
    assertEquals(1, serviceBusSendingExceptions.size());
    assertEquals(SENDER_EXCEPTION_MSG, serviceBusSendingExceptions.get(0).getMessage());
    verify(serviceBusRecord).getOriginalTopic();
    verify(senderFacade, times(MAX_RETRIES)).sendMessages(anyCollection());
  }

  @Test
  void sendMessagesShouldSplitRecordsBetweenSenders() {
    //given
    ServiceBusMessageWrapper serviceBusRecord = mockServiceBusRecordComposite(KAFKA_TOPIC);
    ServiceBusMessageWrapper serviceBusRecord2 = mockServiceBusRecordComposite(KAFKA_TOPIC2);

    Set<ServiceBusMessageWrapper> serviceBusRecords = Set.of(serviceBusRecord, serviceBusRecord2);

    when(senderFacade.sendMessages(anyCollection())).thenReturn(Optional.empty());
    when(senderFacade2.sendMessages(anyCollection())).thenReturn(Optional.empty());

    //when
    //NOTE: two different mappings in senders will delegate each message to its own sender
    testObj =
        new TaskToSenderBridge(SINK_CONFIG, Map.of(KAFKA_TOPIC, senderFacade, KAFKA_TOPIC2, senderFacade2),
            COMMIT_FUNCTION, Map.of(KAFKA_TOPIC, sinkMapping));

    List<ServiceBusSendingException> serviceBusSendingExceptions = testObj.sendMessages(serviceBusRecords);

    //then
    assertTrue(serviceBusSendingExceptions.isEmpty());
    verify(serviceBusRecord).getOriginalTopic();
    verify(senderFacade).sendMessages(argThat(messagesCollection -> messagesCollection.contains(serviceBusRecord)));
    verify(serviceBusRecord2).getOriginalTopic();
    verify(senderFacade2).sendMessages(argThat(messagesCollection -> messagesCollection.contains(serviceBusRecord2)));
  }

  @Test
  void closeSenderClientsShouldCloseEachClient() {
    //when
    testObj.closeSenderClients();

    //then
    verify(senderFacade).close();
  }

  private static ServiceBusMessageWrapper mockServiceBusRecordComposite(String originalTopic) {
    ServiceBusMessageWrapper serviceBusRecord = mock(ServiceBusMessageWrapper.class);
    when(serviceBusRecord.getOriginalTopic()).thenReturn(originalTopic);
    return serviceBusRecord;
  }
}
