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
package io.lenses.streamreactor.connect.azure.eventhubs.config;

import static org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

class SourceDataTypeTest {

  @Test
  void fromNameShouldReturnEnum() {
    //given

    //when
    SourceDataType s = SourceDataType.fromName(SourceDataType.BYTES.name());

    //then
    assertEquals(SourceDataType.BYTES, s);
  }

  @Test
  void getDeserializerClassShouldReturnSpecifiedDeserializer() {
    //given

    //when
    Class<? extends Deserializer> deserializerClass = SourceDataType.BYTES.getDeserializerClass();

    //then
    assertEquals(ByteArrayDeserializer.class, deserializerClass);
  }

  @Test
  void getSchema() {
    //given

    //when
    Schema schema = SourceDataType.BYTES.getSchema();

    //then
    assertEquals(OPTIONAL_BYTES_SCHEMA, schema);
  }
}