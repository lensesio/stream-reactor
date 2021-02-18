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

package com.datamountaineer.streamreactor.connect.blockchain.data

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}

case class Output(addr_tag_link: Option[String],
                  addr_tag: Option[String],
                  spent: Boolean,
                  tx_index: Long,
                  `type`: Int,
                  addr: Option[String],
                  value: Long,
                  n: Int,
                  script: String)

object Output {

  val ConnectSchema: Schema = SchemaBuilder.struct
    .name("datamountaineer.blockchain.output")
    .doc("The output instance part of a transaction.")
    .field("addr_tag_link", Schema.OPTIONAL_STRING_SCHEMA)
    .field("addr_tag", Schema.OPTIONAL_STRING_SCHEMA)
    .field("spent", Schema.BOOLEAN_SCHEMA)
    .field("tx_index", Schema.INT64_SCHEMA)
    .field("type", Schema.INT32_SCHEMA)
    .field("addr", Schema.OPTIONAL_STRING_SCHEMA)
    .field("value", Schema.INT64_SCHEMA)
    .field("n", Schema.INT32_SCHEMA)
    .field("script", Schema.STRING_SCHEMA)
    .build()

  implicit class OutputToStructConverter(val output: Output) extends AnyVal {
    def toStruct(): Struct = {
      val struct = new Struct(ConnectSchema)
        .put("spent", output.spent)
        .put("tx_index", output.tx_index)
        .put("type", output.`type`)
        .put("value", output.value)
        .put("n", output.n)
        .put("script", output.script)
      output.addr.foreach(struct.put("addr", _))
      output.addr_tag.foreach(struct.put("addr_tag", _))
      output.addr_tag_link.foreach(struct.put("addr_tag_link", _))
      struct
    }
  }

}
