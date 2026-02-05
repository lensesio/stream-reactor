/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.config

import io.confluent.connect.avro.AvroData
import io.confluent.connect.avro.AvroDataConfig
import io.confluent.connect.schema.AbstractDataConfig

import scala.jdk.CollectionConverters.MapHasAsJava

/**
  * Factory for creating properly configured AvroData instances.
  * Ensures enhanced.avro.schema.support is enabled across the codebase.
  */
object AvroDataFactory {

  /**
    * Creates an AvroData instance with enhanced schema support enabled.
    *
    * @param cacheSize the schema cache size (default: 100)
    * @return a properly configured AvroData instance
    */
  def create(cacheSize: Int = 100): AvroData = {
    val config = new AvroDataConfig(
      Map(
        AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG -> "true",
        AbstractDataConfig.SCHEMAS_CACHE_SIZE_CONFIG       -> cacheSize.toString,
      ).asJava,
    )
    new AvroData(config)
  }
}
