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
package com.landoop.streamreactor.connect.hive.source.offset

import com.landoop.streamreactor.connect.hive.source.SourcePartition.fromSourcePartition
import com.landoop.streamreactor.connect.hive.source.SourcePartition.toSourceOffset
import com.landoop.streamreactor.connect.hive.source.SourceOffset
import com.landoop.streamreactor.connect.hive.source.SourcePartition
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Try

/**
  * Provides a reader for the Hive offsets from the context to be used on the Source initialisation
  * @param reader the reader provided by the context for retrieving the offsets
  */
class HiveSourceInitOffsetStorageReader(reader: OffsetStorageReader) extends HiveSourceOffsetStorageReader {

  def offset(partition: SourcePartition): Option[SourceOffset] = {
    val offsetMap = reader.offset(fromSourcePartition(partition).asJava)
    Try(toSourceOffset(offsetMap.asScala.toMap)).toOption
  }

}
