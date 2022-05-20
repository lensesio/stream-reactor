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

package io.lenses.streamreactor.connect.aws.s3.model

import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.source.SourceRecordConverter.fromSourceOffset
import io.lenses.streamreactor.connect.aws.s3.source.SourceRecordConverter.fromSourcePartition
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.source.SourceRecord

import scala.jdk.CollectionConverters.MapHasAsJava

abstract class SourceData(lineNumber: Long) {
  def representationSchema: Option[Schema]

  def representationKey: Option[AnyRef]

  def representationValue: AnyRef

  def getLineNumber: Long = lineNumber

  def toSourceRecord(bucketAndPath: RemoteS3PathLocation, targetTopic: String): SourceRecord =
    representationKey match {
      case Some(key) =>
        new SourceRecord(
          fromSourcePartition(bucketAndPath.root()).asJava,
          fromSourceOffset(bucketAndPath, getLineNumber).asJava,
          targetTopic,
          null,
          key,
          representationSchema.orNull,
          representationValue,
        )
      case None =>
        new SourceRecord(
          fromSourcePartition(bucketAndPath.root()).asJava,
          fromSourceOffset(bucketAndPath, getLineNumber).asJava,
          targetTopic,
          representationSchema.orNull,
          representationValue,
        )
    }
}

case class SchemaAndValueSourceData(
  data:       SchemaAndValue,
  lineNumber: Long,
) extends SourceData(lineNumber) {

  override def representationSchema: Option[Schema] = Some(data.schema())

  override def representationKey: Option[AnyRef] = None

  override def representationValue: AnyRef = data.value()
}

case class StringSourceData(
  data:       String,
  lineNumber: Long,
) extends SourceData(lineNumber) {
  override def representationSchema: Option[Schema] = None

  override def representationKey: Option[AnyRef] = None

  override def representationValue: AnyRef = data

}

case class ByteArraySourceData(
  data:       BytesOutputRow,
  lineNumber: Long,
) extends SourceData(lineNumber) {
  override def representationSchema: Option[Schema] = None

  override def representationKey: Option[AnyRef] = Some(data.key)

  override def representationValue: AnyRef = data.value
}
