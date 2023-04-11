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

import scala.util.Try

/**
  * A [[PartitionHandler]] is invoked to handle resolution of
  * partitions when writing data via a [[HiveSink]].
  *
  * More specifically, the policy is used to determine the location
  * of where files for a partition should be written to on disk, as well
  * ensuring that partition is present in the hive-metastore.
  *
  * If a partition does not exist, then the implementation
  * is free to create the partition, or return an error.
  *
  * The most common strategies when using the hive command line
  * are dynamic partitioning (where the partitions are created at
  * execution time) and static partitioning (where the partitions
  * must be specified by the user in the query).
  * (see https://cwiki.apache.org/confluence/display/Hive/DynamicPartitions)
  *
  * Kafka-connect-hive duplicates this behavior by offering a
  * [[DynamicPartitionHandler]] policy where the partitions will be
  * created as required and a [[StrictPartitionHandler]] policy
  * where the partitions must already exist in the metastore.
  *
  * Users are free to implement any other policy they require
  * by implementing this trait. For example a custom policy may
  * decide to locate partitions in a non-standard directory format,
  * or only allow partition names that match a pre-determined format.
  */
trait PartitionHandler {
  def path(
    partition: Partition,
    db:        DatabaseName,
    tableName: TableName,
  )(client:    IMetaStoreClient,
    fs:        FileSystem,
  ): Try[Path]
}
