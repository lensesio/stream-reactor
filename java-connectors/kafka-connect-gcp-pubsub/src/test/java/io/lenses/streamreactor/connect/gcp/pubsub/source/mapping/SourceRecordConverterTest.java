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
package io.lenses.streamreactor.connect.gcp.pubsub.source.mapping;

import static io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.MappingConfig.COMPATIBILITY_MAPPING_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;

import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.PubSubMessageData;
import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.SourceOffset;
import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.SourcePartition;
import lombok.val;

class SourceRecordConverterTest {

  private static final Map<String, String> ATTRIBUTE_MAP =
      Map.of(
          "key", "value"
      );

  private static final String ATTRIBUTE_MAP_STRING = "{\"key\":\"value\"}";

  @Test
  void shouldConvertDataForCompatibilityMode() throws UnsupportedEncodingException {
    val converted = new SourceRecordConverter(COMPATIBILITY_MAPPING_CONFIG).convert(setUpDataForCompatibilityMode());

    assertEquals(Map.of("message.id", "messageId1"), converted.sourceOffset());
    assertEquals(Map.of(
        "project.id", "projectId1",
        "topic.id", "topicId1",
        "subscription.id", "subscriptionId1"),
        converted.sourcePartition());
    assertEquals("targetTopicName", converted.topic());

    val connectRecord = (Struct) converted.value();
    assertEquals("My data", connectRecord.get("MessageData"));
    assertEquals(ATTRIBUTE_MAP_STRING, connectRecord.get("AttributeMap"));
  }

  private static PubSubMessageData setUpDataForCompatibilityMode() throws UnsupportedEncodingException {
    return new PubSubMessageData(
        new SourcePartition("projectId1", "topicId1", "subscriptionId1"),
        new SourceOffset("messageId1"),
        PubsubMessage.newBuilder()
            .setMessageId("messageId")
            .setData(
                ByteString.copyFrom("My data", "UTF-8")
            )
            .setPublishTime(Timestamp.newBuilder().build())
            .putAllAttributes(
                ATTRIBUTE_MAP
            ).build(),
        "targetTopicName"
    );
  }

}
