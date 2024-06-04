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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.connect.azure.servicebus.util.ServiceBusKcqlProperties;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.junit.jupiter.api.Test;

class ServiceBusReceiverFacadeTest {

  private static final String CONNECTION_STRING =
      "Endpoint=sb://TESTENDPOINT.servicebus.windows.net/;"
          + "SharedAccessKeyName=EXAMPLE_NAME;SharedAccessKey=EXAMPLE_KEY";
  private static final String SOME_RECEIVER_ID = "REC-IEVER-ID";
  private final BlockingQueue<ServiceBusMessageHolder> mockedQueue = mock(BlockingQueue.class);

  @Test
  void checkReceiverInitializationAndReceiverIdForQueue() {
    //given
    String queueType = "QUEUE";
    Map<String, String> propertiesMap = mock(Map.class);
    when(propertiesMap.get(ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName())).thenReturn(queueType);

    Kcql kcql = mockKcql(propertiesMap);

    ServiceBusReceiverFacade testObj =
        new ServiceBusReceiverFacade(kcql, mockedQueue,
            CONNECTION_STRING, SOME_RECEIVER_ID);

    //when
    String receiverId = testObj.getReceiverId();

    //then
    verify(kcql).getProperties();
    verify(kcql).getSource();
    verify(propertiesMap).get(ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName());
    assertThat(receiverId).isEqualTo(SOME_RECEIVER_ID);
  }

  @Test
  void checkReceiverInitializationAndReceiverIdForTopic() {
    //given
    String topicType = "TOPIC";
    String subscriptionName = "SUBSCRIPTION";
    Map<String, String> propertiesMap = mock(Map.class);
    when(propertiesMap.get(ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName())).thenReturn(topicType);
    when(propertiesMap.get(ServiceBusKcqlProperties.SUBSCRIPTION_NAME.getPropertyName())).thenReturn(subscriptionName);

    Kcql kcql = mockKcql(propertiesMap);

    ServiceBusReceiverFacade testObj =
        new ServiceBusReceiverFacade(kcql, mockedQueue,
            CONNECTION_STRING, SOME_RECEIVER_ID);

    //when
    String receiverId = testObj.getReceiverId();

    //then
    verify(kcql).getProperties();
    verify(kcql).getSource();
    verify(propertiesMap).get(ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName());
    verify(propertiesMap).get(ServiceBusKcqlProperties.SUBSCRIPTION_NAME.getPropertyName());
    assertThat(receiverId).isEqualTo(SOME_RECEIVER_ID);
  }

  @Test
  void shouldGetIllegalArgumentExceptionOnCreationWithBadlyFormattedConnectionString() {
    //given
    Kcql kcql = mock(Kcql.class);
    String badFormatConnectionString = "connectionString";

    //when
    assertThrows(IllegalArgumentException.class, () -> new ServiceBusReceiverFacade(kcql, mockedQueue,
        badFormatConnectionString, SOME_RECEIVER_ID)
    );
  }

  @Test
  void completeShouldCallRecieverComplete() {
    //given
    Kcql kcql = mock(Kcql.class);
    ServiceBusReceiverAsyncClient receiverAsyncClient = mock(ServiceBusReceiverAsyncClient.class);
    ServiceBusReceivedMessage mockedMessage = mock(ServiceBusReceivedMessage.class);

    //when
    ServiceBusReceiverFacade serviceBusReceiverFacade =
        new ServiceBusReceiverFacade(kcql, mockedQueue, CONNECTION_STRING, SOME_RECEIVER_ID, receiverAsyncClient);
    serviceBusReceiverFacade.complete(mockedMessage);

    //then
    verify(receiverAsyncClient).complete(mockedMessage);
  }

  private static Kcql mockKcql(Map<String, String> propertiesMap) {
    String inputBusName = "INPUT";
    Kcql kcql = mock(Kcql.class);
    when(kcql.getSource()).thenReturn(inputBusName);
    when(kcql.getProperties()).thenReturn(propertiesMap);
    return kcql;
  }
}
