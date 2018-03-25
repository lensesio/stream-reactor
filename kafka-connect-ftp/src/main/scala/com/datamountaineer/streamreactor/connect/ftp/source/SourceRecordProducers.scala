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

package com.datamountaineer.streamreactor.connect.ftp.source

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord

/**
  * Created by andrew@datamountaineer.com on 31/08/2017. 
  * Default (Template) Project
  */
object SourceRecordProducers {
  type SourceRecordProducer = (ConnectFileMetaDataStore, String, FileMetaData, FileBody) => SourceRecord

  val fileInfoSchema = SchemaBuilder.struct()
    .field("name", Schema.STRING_SCHEMA)
    .field("offset", Schema.INT64_SCHEMA)
    .build()

  def stringKeyRecord(store: ConnectFileMetaDataStore, topic: String, meta: FileMetaData, body: FileBody): SourceRecord =
    new SourceRecord(
      store.fileMetasToConnectPartition(meta), // source part
      store.fileMetasToConnectOffset(meta), // source off
      topic, //topic
      Schema.STRING_SCHEMA, // key sch
      meta.attribs.path, // key
      Schema.BYTES_SCHEMA, // val sch
      body.bytes // val
    )

  def structKeyRecord(store: ConnectFileMetaDataStore, topic: String, meta: FileMetaData, body: FileBody): SourceRecord = {
    new SourceRecord(
      store.fileMetasToConnectPartition(meta), // source part
      store.fileMetasToConnectOffset(meta), // source off
      topic, //topic
      fileInfoSchema, // key sch
      new Struct(fileInfoSchema)
        .put("name",meta.attribs.path)
        .put("offset",body.offset),
      Schema.BYTES_SCHEMA, // val sch
      body.bytes // val
    )
  }
}
