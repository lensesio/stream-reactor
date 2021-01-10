/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink.commit

import io.lenses.streamreactor.connect.aws.s3.config.CommitMode
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPrefix, PartitionField, TopicPartitionOffset}
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.storage.Storage

/**
  * Interfaces for committing the records to S3 and ensure exactly once semantics
  */
trait Committer extends WatermarkSeeker {
  def commit(bucketAndPrefix: BucketAndPrefix,
             topicPartitionOffset: TopicPartitionOffset,
             partitionValues: Map[PartitionField, String]): Unit
}


object Committer {
  def from(config: S3SinkConfig, storage: Storage): Committer = {
    config.commitMode match {
      case CommitMode.Gen1 => Gen1Committer.from(config, storage)
      case CommitMode.Gen2 => Gen2Committer.from(config, storage)
    }
  }
}