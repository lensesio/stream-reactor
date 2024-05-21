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

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.google.gson.Gson;

import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.PubSubMessageData;

/**
 * CompatibilityValueMapping is responsible for mapping the value from PubSubMessageData to Kafka Connect value. This is
 * for a mode that aims to enable compatibility with another connector in the market.
 */
public class CompatibilityValueMapper implements ValueMapper {

  private static final String STRUCT_KEY_MESSAGE_DATA = "MessageData";
  private static final String STRUCT_KEY_ATTRIBUTE_MAP = "AttributeMap";
  private static final Schema SCHEMA =
      SchemaBuilder
          .struct()
          .field(STRUCT_KEY_MESSAGE_DATA, Schema.OPTIONAL_STRING_SCHEMA)
          .field(STRUCT_KEY_ATTRIBUTE_MAP, Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  private final Gson gson = new Gson();

  @Override
  public Object mapValue(final PubSubMessageData source) {

    return new Struct(SCHEMA)
        .put(STRUCT_KEY_MESSAGE_DATA, new String(source.getMessage().getData().toByteArray()))
        .put(STRUCT_KEY_ATTRIBUTE_MAP, attributesMapToString(source.getMessage().getAttributesMap()));
  }

  private String attributesMapToString(Map<String, String> attributesMap) {
    return Optional
        .ofNullable(attributesMap)
        .map(gson::toJson)
        .orElse(null);
  }

  @Override
  public Schema getSchema() {
    return SCHEMA;
  }
}
