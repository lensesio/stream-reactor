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
package com.landoop.streamreactor.connect.hive.sink.partitioning

import com.landoop.streamreactor.connect.hive.Partition
import com.landoop.streamreactor.connect.hive.PartitionKey
import org.apache.hadoop.fs.Path

/**
  * Strategy interface used to determine where new partitions are written.
  */
trait PartitionPathPolicy {
  def path(tableLocation: Path, partition: Partition): Path
}

object PartitionPathPolicy {
  implicit val default = DefaultMetastorePartitionPathPolicy
}

/**
  * The default compile of PartitionLocationPolicy which uses
  * the same method as the metastore. That is new partitions will be written
  * under the table location in a key1=value1/key2=value2 format.
  */
object DefaultMetastorePartitionPathPolicy extends PartitionPathPolicy {
  override def path(tableLocation: Path, partition: Partition): Path =
    partition.entries.foldLeft(tableLocation) {
      case (path, (PartitionKey(key), value)) =>
        new Path(path, s"$key=$value")
    }
}
