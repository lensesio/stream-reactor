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
import static org.assertj.core.api.Assertions.from;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusMessage;
import io.lenses.streamreactor.connect.azure.servicebus.sink.ServiceBusMessageWrapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class SinkRecordToServiceBusMapperTest {

  private static final String SOME_DATA = "SOMEDATA";
  private static final String MSGID = "MSGID";
  private static final String BINARY_TYPE = "binary";
  private static final long ORIGINAL_OFFSET = 100L;
  private static final int ORIGINAL_PARTITION = 10;
  private static final String ORIGINAL_TOPIC = "ORIGINAL_TOPIC";

  @Test
  void mapToServiceBusMessageShouldReturnServiceBusMessageComposite() {
    //given
    SinkRecord sinkRecord = mock(SinkRecord.class);
    when(sinkRecord.value()).thenReturn(SOME_DATA);
    when(sinkRecord.keySchema()).thenReturn(Schema.STRING_SCHEMA);
    when(sinkRecord.key()).thenReturn(MSGID);
    when(sinkRecord.originalTopic()).thenReturn(ORIGINAL_TOPIC);
    when(sinkRecord.originalKafkaPartition()).thenReturn(ORIGINAL_PARTITION);
    when(sinkRecord.originalKafkaOffset()).thenReturn(ORIGINAL_OFFSET);

    //when
    ServiceBusMessageWrapper composite = SinkRecordToServiceBusMapper.mapToServiceBus(sinkRecord);

    //then
    verify(sinkRecord).value();
    assertTrue(composite.getServiceBusMessage().isPresent());
    assertThat(composite.getServiceBusMessage().get())
        .returns(BinaryData.fromObject(SOME_DATA).toString(), from(busMessage -> busMessage.getBody().toString()))
        .returns(MSGID, from(ServiceBusMessage::getMessageId))
        .returns(BINARY_TYPE, from(ServiceBusMessage::getContentType));
    assertEquals(ORIGINAL_OFFSET, composite.getOriginalKafkaOffset());
    assertEquals(ORIGINAL_PARTITION, composite.getOriginalKafkaPartition());
    assertEquals(ORIGINAL_TOPIC, composite.getOriginalTopic());
  }
}
