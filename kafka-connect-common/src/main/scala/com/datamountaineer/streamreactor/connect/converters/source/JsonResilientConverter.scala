/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.connect.converters.source

import java.util
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.json.JsonConverter

import scala.util.Try

/**
  * A Json converter built with resilience, meaning that malformed Json messages are now ignored
  */
class JsonResilientConverter extends JsonConverter {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
    super.configure(configs, isKey)

  override def fromConnectData(topic: String, schema: Schema, value: Object): Array[Byte] =
    Try {
      super.fromConnectData(topic, schema, value)
    }.toOption.orNull

  override def toConnectData(topic: String, value: Array[Byte]): SchemaAndValue =
    Try {
      super.toConnectData(topic, value)
    }.getOrElse {
      SchemaAndValue.NULL
    }
}
