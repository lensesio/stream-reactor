/*
 * Copyright 2017 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.rethink.source

import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}

/**
  * Created by andrew@datamountaineer.com on 22/09/16. 
  * stream-reactor
  */

object ChangeFeedStructBuilder extends StrictLogging {

  val mapper = new ObjectMapper()
  val oldVal = "old_val"
  val newVal = "new_val"
  val state = "state"
  val `type` = "type"

  val schema: Schema = SchemaBuilder.struct.name("ReThinkChangeFeed")
    .version(1)
    .field(state, Schema.OPTIONAL_STRING_SCHEMA)
    .field(oldVal, Schema.OPTIONAL_STRING_SCHEMA)
    .field(newVal, Schema.OPTIONAL_STRING_SCHEMA)
    .field(`type`, Schema.OPTIONAL_STRING_SCHEMA)
    .build

  def apply(hm: Map[String, Object]): Struct = {
    val struct = new Struct(schema)
    hm.foreach({ case (k, v) => if (v != null) struct.put(k, v.toString) })
    struct
  }
}
