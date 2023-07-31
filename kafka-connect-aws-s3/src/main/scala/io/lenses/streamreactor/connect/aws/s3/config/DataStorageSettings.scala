/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.config

import cats.implicits.catsSyntaxEitherId
import org.apache.kafka.common.config.ConfigException

/**
  * Flags for enabling/disabling storage of various fields in a Kafka message.
  * Values are always stored
  *
  * @param keys - If enabled it stores the keys of the Kafka message
  * @param metadata - If enabled it stores the metadata of the Kafka message
  * @param headers - If enabled it stores the headers of the Kafka message
  */
case class DataStorageSettings(keys: Boolean, metadata: Boolean, headers: Boolean) {
  private val _isDataStored = keys || metadata || headers
  def isDataStored: Boolean = _isDataStored
}

object DataStorageSettings {
  private val StoreKeysKey     = "store.keys"
  private val StoreMetadataKey = "store.metadata"
  private val StoreHeadersKey  = "store.headers"
  def from(properties: Map[String, String]): Either[Throwable, DataStorageSettings] =
    for {
      keys     <- getBoolean(StoreKeysKey, properties, false)
      metadata <- getBoolean(StoreMetadataKey, properties, false)
      headers  <- getBoolean(StoreHeadersKey, properties, false)
    } yield {
      DataStorageSettings(keys, metadata, headers)
    }

  private def getBoolean(key: String, properties: Map[String, String], default: Boolean): Either[Throwable, Boolean] =
    properties.get(key) match {
      case Some(value) =>
        value match {
          case "true"  => true.asRight[Throwable]
          case "false" => false.asRight[Throwable]
          case _       => new ConfigException(s"Invalid value for $key. Must be true or false").asLeft[Boolean]
        }
      case None => default.asRight[Throwable]
    }
}
