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

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;

/**
 * Class to indicate what kind of data is being received from Kafka Consumer.
 */
@Getter
public enum SourceDataType {

  BYTES(ByteArrayDeserializer.class, Schema.OPTIONAL_BYTES_SCHEMA);

  private final Class<? extends Deserializer> deserializerClass;
  private final Schema schema;
  private static final Map<String, SourceDataType> NAME_TO_DATA_SERIALIZER_TYPE;

  static {
    NAME_TO_DATA_SERIALIZER_TYPE =
        Arrays.stream(values()).collect(Collectors.toMap(Enum::name, Function.identity()));
  }

  SourceDataType(Class<? extends Deserializer> deserializerClass, Schema schema) {
    this.deserializerClass = deserializerClass;
    this.schema = schema;
  }

  public static SourceDataType fromName(String name) {
    return NAME_TO_DATA_SERIALIZER_TYPE.get(name.toUpperCase());
  }

  /**
   * Class indicates what data types are being transferred by Task.
   */
  @Getter
  @EqualsAndHashCode
  public static class KeyValueTypes {
    private final SourceDataType keyType;
    private final SourceDataType valueType;
    public static final KeyValueTypes DEFAULT_TYPES = new KeyValueTypes(BYTES, BYTES);

    public KeyValueTypes(SourceDataType keyType, SourceDataType valueType) {
      this.keyType = keyType;
      this.valueType = valueType;
    }
  }
}
