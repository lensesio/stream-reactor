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
package com.landoop.streamreactor.connect.hive.source

import com.landoop.streamreactor.connect.hive.DatabaseName
import com.landoop.streamreactor.connect.hive.TableName
import com.landoop.streamreactor.connect.hive.Topic
import org.apache.hadoop.fs.Path

case class SourcePartition(db: DatabaseName, tableName: TableName, topic: Topic, path: Path)

case class SourceOffset(rowNumber: Int)

object SourcePartition {

  def fromSourcePartition(partition: SourcePartition): Map[String, AnyRef] =
    Map[String, AnyRef](
      "db"    -> partition.db.value,
      "table" -> partition.tableName.value,
      "topic" -> partition.topic.value,
      "path"  -> partition.path.toString,
    )

  def toSourcePartition(map: Map[String, AnyRef]): SourcePartition =
    SourcePartition(
      DatabaseName(map("db").toString),
      TableName(map("table").toString),
      Topic(map("topic").toString),
      new Path(map("path").toString),
    )

  def fromSourceOffset(offset: SourceOffset): Map[String, AnyRef] =
    Map("rownum" -> offset.rowNumber.toString)

  def toSourceOffset(map: Map[String, AnyRef]): SourceOffset =
    SourceOffset(map("rownum").toString.toInt)
}
