/*
 * Copyright 2021 Lenses.io
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

import io.lenses.streamreactor.connect.aws.s3.model.{ByteArraySourceData, PollResults, SchemaAndValueSourceData, SourceData, StringSourceData}
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import org.apache.kafka.connect.data.{Schema, SchemaAndValue}
import org.apache.kafka.connect.source.SourceRecord

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

object SourceRecordConverter {

  def fromSourcePartition(bucket: String, prefix: String) =
    Map(
      "container" -> bucket,
      "prefix" -> prefix
    )

  private def fromSourceOffset(bucketAndPath: RemoteS3PathLocation, offset: Long): Map[String, AnyRef] =
    Map(
      "path" -> bucketAndPath.path,
      "line" -> offset.toString
    )

  def convertToSourceRecordList(pollResults: PollResults): Vector[SourceRecord] =
    pollResults.resultList.map(convertToSourceRecord(_, pollResults))

  private def convertToSourceRecord(sourceData: SourceData, pollResults: PollResults): SourceRecord = {

    val (schema: Option[Schema], value: AnyRef, key: Option[AnyRef]) = sourceData match {
      case ByteArraySourceData(resultBytes, _) =>
        (None, resultBytes.value, Some(resultBytes.key))
      case SchemaAndValueSourceData(result: SchemaAndValue, _) =>
        (Some(result.schema()), result.value(), None)
      case StringSourceData(result: String, _) =>
        (None, result, None)
      case _ =>
        throw new IllegalArgumentException(s"Unexpected type in convertToSourceRecord, ${sourceData.getClass}")
    }
    if (key.isDefined) {
      new SourceRecord(
        fromSourcePartition(pollResults.bucketAndPath.bucket, pollResults.prefix).asJava,
        fromSourceOffset(pollResults.bucketAndPath, sourceData.getLineNumber).asJava,
        pollResults.targetTopic,
        null,
        key.orNull,
        schema.orNull,
        value
      )
    } else {
      new SourceRecord(
        fromSourcePartition(pollResults.bucketAndPath.bucket, pollResults.prefix).asJava,
        fromSourceOffset(pollResults.bucketAndPath, sourceData.getLineNumber).asJava,
        pollResults.targetTopic,
        schema.orNull,
        value
      )
    }

  }

}
