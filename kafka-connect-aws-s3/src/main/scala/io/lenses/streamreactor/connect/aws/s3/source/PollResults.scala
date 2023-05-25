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
package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits.toTraverseOps
import io.lenses.streamreactor.connect.aws.s3.formats.reader.SourceData
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import org.apache.kafka.connect.source.SourceRecord

case class PollResults(
  resultList:    Vector[_ <: SourceData],
  bucketAndPath: S3Location,
  targetTopic:   String,
  partitionFn:   String => Option[Int],
) {

  def toSourceRecordList: Either[Throwable, Seq[SourceRecord]] =
    resultList.map(_.toSourceRecord(bucketAndPath, targetTopic, partitionFn)).toList.traverse(identity)

}
