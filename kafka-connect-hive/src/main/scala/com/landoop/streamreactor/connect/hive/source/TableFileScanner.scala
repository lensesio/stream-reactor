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

import com.landoop.streamreactor.connect.hive
import com.landoop.streamreactor.connect.hive.HdfsUtils._
import com.landoop.streamreactor.connect.hive.DatabaseName
import com.landoop.streamreactor.connect.hive.Partition
import com.landoop.streamreactor.connect.hive.TableName
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient

// for a table that is not partitioned, the files will all reside directly in the table location directory
// otherwise, the files will each live in the particular partition folder (which technically, could be anywhere)
object TableFileScanner {

  def scan(
    db:        DatabaseName,
    tableName: TableName,
  )(
    implicit
    fs:     FileSystem,
    client: IMetaStoreClient,
  ): Seq[(Path, Option[Partition])] =
    // the partitions from the metastore which each contain a pointer to the partition location
    hive.partitionPlan(db, tableName) match {
      case Some(plan @ _) =>
        hive.partitions(db, tableName).flatMap {
          case partition @ Partition(entries @ _, Some(location)) =>
            val files = fs.listFiles(location, false)
            files.map(_.getPath).toVector.map(_ -> Some(partition))
          case other => throw new IllegalStateException(s"No match for other $other in scan")
        }
      case None =>
        val table = client.getTable(db.value, tableName.value)
        val files = fs.listFiles(new Path(table.getSd.getLocation), false)
        files.map(_.getPath).toVector.map(_ -> None)
    }
}
