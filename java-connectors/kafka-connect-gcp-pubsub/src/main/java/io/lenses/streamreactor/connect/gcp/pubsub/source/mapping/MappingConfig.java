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

import io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.headers.HeaderMapping;
import io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.headers.MinimalAndMessageAttributesHeaderMapping;
import io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.headers.MinimalHeaderMapping;
import io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.key.CompatibilityKeyMapping;
import io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.key.KeyMapping;
import io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.key.MessageIdKeyMapping;
import io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.value.CompatibilityValueMapping;
import io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.value.MessageValueMapping;
import io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.value.ValueMapping;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * MappingConfig is responsible for holding the mapping configuration for the PubSubMessageData to SourceRecord
 * conversion.
 * It contains the key, value and header mapping configurations.
 */
@AllArgsConstructor
@Getter
public class MappingConfig {

  public static final String OUTPUT_MODE_DEFAULT = "DEFAULT";
  public static final String OUTPUT_MODE_COMPATIBILITY = "COMPATIBILITY";
  private KeyMapping keyMapper;

  private ValueMapping valueMapper;

  private HeaderMapping headerMapper;

  public static MappingConfig fromOutputMode(String outputMode) {
    switch (outputMode.toUpperCase()) {
      case OUTPUT_MODE_COMPATIBILITY:
        return MappingConfig.COMPATIBILITY_MAPPING_CONFIG;
      default:
      case OUTPUT_MODE_DEFAULT:
        return MappingConfig.DEFAULT_MAPPING_CONFIG;
    }
  }

  public static final MappingConfig DEFAULT_MAPPING_CONFIG =
      new MappingConfig(
          new MessageIdKeyMapping(),
          new MessageValueMapping(),
          new MinimalAndMessageAttributesHeaderMapping()
      );

  public static final MappingConfig COMPATIBILITY_MAPPING_CONFIG =
      new MappingConfig(
          new CompatibilityKeyMapping(),
          new CompatibilityValueMapping(),
          new MinimalHeaderMapping()
      );

}
