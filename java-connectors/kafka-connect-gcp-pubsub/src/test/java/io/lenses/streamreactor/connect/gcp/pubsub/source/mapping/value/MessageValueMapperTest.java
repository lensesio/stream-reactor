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
package io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.value;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.PubSubMessageData;
import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.PubSubSourceOffset;
import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.PubSubSourcePartition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MessageValueMapperTest {

  @Mock
  PubSubSourcePartition sourcePartition;
  @Mock
  PubSubSourceOffset sourceOffset;

  @Test
  void testGetValue() {
    String testMessageData = "Test message data";
    PubsubMessage message =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(testMessageData))
            .build();
    PubSubMessageData pubSubMessageData =
        new PubSubMessageData(
            sourcePartition,
            sourceOffset,
            message,
            "notRelevantToThisTest"
        );

    MessageValueMapper messageValueMapping = new MessageValueMapper();
    byte[] result = (byte[]) messageValueMapping.mapValue(pubSubMessageData);

    assertArrayEquals(testMessageData.getBytes(), result);
  }

  @Test
  void testGetSchema() {
    MessageValueMapper messageValueMapping = new MessageValueMapper();
    Schema result = messageValueMapping.getSchema();

    assertEquals(Schema.BYTES_SCHEMA, result);
  }
}
