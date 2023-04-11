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

import com.landoop.streamreactor.connect.hive.DatabaseName
import com.landoop.streamreactor.connect.hive.Partition
import com.landoop.streamreactor.connect.hive.TableName
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.util.Success
import scala.util.Try

/**
  * A [[PartitionHandler]] that delegates to an underlying policy
  * and caches the results for the lifetime of this object.
  */
class CachedPartitionHandler(partitioner: PartitionHandler) extends PartitionHandler {

  val cache = scala.collection.mutable.Map.empty[Partition, Path]

  override def path(
    partition: Partition,
    db:        DatabaseName,
    tableName: TableName,
  )(client:    IMetaStoreClient,
    fs:        FileSystem,
  ): Try[Path] =
    cache.get(partition) match {
      case Some(path) => Success(path)
      case _ =>
        val created = partitioner.path(partition, db, tableName)(client, fs)
        created.foreach(cache.put(partition, _))
        created
    }
}
