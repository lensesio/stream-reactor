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
package io.lenses.streamreactor.connect.azure.servicebus.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lenses.streamreactor.connect.azure.servicebus.source.ServiceBusPartitionOffsetProvider;
import io.lenses.streamreactor.connect.azure.servicebus.source.ServiceBusPartitionOffsetProvider.AzureServiceBusOffsetMarker;
import io.lenses.streamreactor.connect.azure.servicebus.source.ServiceBusPartitionOffsetProvider.AzureServiceBusPartitionKey;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ServiceBusPartitionOffsetProviderTest {

  private static final String MESSAGE_ID_KEY = "MESSAGE_ID";
  private OffsetStorageReader offsetStorageReader;

  private ServiceBusPartitionOffsetProvider testObj;

  @BeforeEach
  void setUp() {
    offsetStorageReader = mock(OffsetStorageReader.class);
    testObj = new ServiceBusPartitionOffsetProvider(offsetStorageReader);
  }

  @Test
  void getOffsetShouldReturnEmptyOptionalIfOffsetNotPresent() {
    //given
    String someTopic = "TOPIC";
    String partition = "PARTITION";
    when(offsetStorageReader.offset(any(AzureServiceBusPartitionKey.class))).thenReturn(null);

    //when
    Optional<AzureServiceBusOffsetMarker> offset =
        testObj.getOffset(new AzureServiceBusPartitionKey(someTopic, partition));

    //then
    verify(offsetStorageReader).offset(any(AzureServiceBusPartitionKey.class));
    assertThat(offset).isNotNull();
    assertThat(offset.isEmpty()).isTrue();
  }

  @Test
  void getOffsetShouldReturnEmptyOptionalOffsetIfPresent() {
    //given
    String someTopic = "TOPIC";
    String partition = "PARTITION";
    Long exampleOffset = 100L;
    Map<String, Object> offsetMap = Map.of(MESSAGE_ID_KEY, exampleOffset);
    AzureServiceBusPartitionKey azureServiceBusPartitionKey = new AzureServiceBusPartitionKey(someTopic, partition);
    when(offsetStorageReader.offset(azureServiceBusPartitionKey)).thenReturn(offsetMap);

    //when
    Optional<AzureServiceBusOffsetMarker> offset =
        testObj.getOffset(azureServiceBusPartitionKey);

    //then
    verify(offsetStorageReader).offset(any(AzureServiceBusPartitionKey.class));
    assertThat(offset).isNotNull();
    assertThat(offset.isEmpty()).isFalse();
    assertThat(offset.get().getOffsetValue()).isEqualTo(exampleOffset);
  }
}
