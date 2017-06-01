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

import java.util
import java.util.Collections

import com.datamountaineer.streamreactor.connect.blockchain.data.Input._
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord

case class Transaction(lock_time: Long,
                       ver: Int,
                       size: Long,
                       inputs: Seq[Input],
                       rbf: Option[Boolean],
                       time: Long,
                       tx_index: Long,
                       vin_sz: Int,
                       hash: String,
                       vout_sz: Int,
                       relayed_by: String,
                       out: Seq[Output])


object Transaction {
  val ConnectSchema: Schema = SchemaBuilder.struct
    .name("datamountaineer.blockchain.transaction")
    .field("lock_time", Schema.INT64_SCHEMA)
    .field("ver", Schema.INT32_SCHEMA)
    .field("size", Schema.INT64_SCHEMA)
    .field("inputs", SchemaBuilder.array(Input.ConnectSchema).optional().build())
    .field("rbf", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("time", Schema.INT64_SCHEMA)
    .field("tx_index", Schema.INT64_SCHEMA)
    .field("vin_sz", Schema.INT32_SCHEMA)
    .field("hash", Schema.STRING_SCHEMA)
    .field("vout_sz", Schema.INT32_SCHEMA)
    .field("relayed_by", Schema.STRING_SCHEMA)
    .field("out", SchemaBuilder.array(Output.ConnectSchema).optional().build())
    .build()

  implicit class TransactionToSourceRecordConverter(val tx: Transaction) extends AnyVal {
    def toSourceRecord(topic: String, partition: Int, key: Option[String]): SourceRecord = {
      new SourceRecord(
        null,
        null,
        topic,
        partition,
        key.map(_ => Schema.STRING_SCHEMA).orNull,
        key.orNull,
        ConnectSchema,
        tx.toStruct()
      )
    }

    //private def getOffset() = Collections.singletonMap("position", System.currentTimeMillis())

    def toStruct(): Struct = {
      val struct = new Struct(ConnectSchema)
        .put("lock_time", tx.lock_time)
        .put("ver", tx.ver)
        .put("size", tx.size)
        .put("time", tx.time)
        .put("tx_index", tx.tx_index)
        .put("vin_sz", tx.vin_sz)
        .put("hash", tx.hash)
        .put("vout_sz", tx.vout_sz)
        .put("relayed_by", tx.relayed_by)

      tx.out.headOption.foreach { _ =>
        import scala.collection.JavaConverters._
        struct.put("out", tx.out.map(_.toStruct()).asJava)
      }
      tx.rbf.foreach(struct.put("rbf", _))
      tx.inputs.headOption.foreach { _ =>
        val inputs = new util.ArrayList[Struct]
        tx.inputs.foreach(i => inputs.add(i.toStruct()))
        struct.put("inputs", inputs)
      }
      tx.out.headOption.foreach { _ =>
        val outputs = new util.ArrayList[Struct]
        tx.out.foreach(output => outputs.add(output.toStruct()))
      }

      struct
    }
  }

}
