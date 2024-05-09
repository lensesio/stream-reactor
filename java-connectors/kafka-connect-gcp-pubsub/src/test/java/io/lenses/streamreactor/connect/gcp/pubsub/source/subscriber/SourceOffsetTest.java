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
package io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

class SourceOffsetTest {

  @Test
  void testFromMap() {
    Map<String, String> sourceLocation = Map.of("message.id", "messageId1");
    SourceOffset sourceOffset = SourceOffset.fromMap(sourceLocation);

    assertEquals("messageId1", sourceOffset.getMessageId());
  }

  @Test
  void testToMap() {
    SourceOffset sourceOffset = new SourceOffset("messageId1");
    Map<String, String> result = sourceOffset.toMap();

    assertEquals(Map.of("message.id", "messageId1"), result);
  }
}
