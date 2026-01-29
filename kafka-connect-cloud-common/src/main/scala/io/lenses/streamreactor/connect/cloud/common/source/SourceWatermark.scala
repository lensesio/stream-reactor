/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.source

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.ContextConstants.ContainerKey
import io.lenses.streamreactor.connect.cloud.common.source.ContextConstants.LastLine
import io.lenses.streamreactor.connect.cloud.common.source.ContextConstants.LineKey
import io.lenses.streamreactor.connect.cloud.common.source.ContextConstants.PathKey
import io.lenses.streamreactor.connect.cloud.common.source.ContextConstants.PrefixKey
import io.lenses.streamreactor.connect.cloud.common.source.ContextConstants.TimeStampKey
import org.apache.kafka.connect.header.ConnectHeaders

import java.time.Instant
import java.util
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

object SourceWatermark {

  /**
    * Builds the source partition information from the CloudLocation
    * @param root The CloudLocation
    * @return A map of partition information
    */
  def partition(root: CloudLocation): java.util.Map[String, String] =
    Map(
      ContainerKey -> root.bucket,
      PrefixKey    -> root.prefixOrDefault(),
    ).asJava

  /**
    * Builds the source offset information from the CloudLocation
    * @param bucketAndPath The CloudLocation
    * @param offset The offset
    * @param lastModified The last modified time of the file processed
    * @return A map of offset information
    */
  def offset(
    bucketAndPath: CloudLocation,
    offset:        Long,
    lastModified:  Instant,
    lastLine:      Boolean,
  ): java.util.Map[String, String] =
    Map(
      PathKey      -> bucketAndPath.pathOrUnknown,
      LineKey      -> offset.toString,
      TimeStampKey -> lastModified.toEpochMilli.toString,
      LastLine     -> ContextBoolean.booleanToString(lastLine),
    ).asJava

  /**
    * Converts a partition map to a CloudLocation object.
    *
    * @param partitionMap A map containing partition information.
    * @param cloudLocationValidator An implicit CloudLocationValidator.
    * @return An Option containing the CloudLocation object if the conversion is successful, None otherwise.
    */
  def partitionMapToSourceRoot(
    partitionMap: Map[String, _],
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): Option[CloudLocation] =
    for {
      bucket <- partitionMap.get(ContainerKey).collect {
        case value: String => value
      }
      prefix = partitionMap.get(PrefixKey).collect {
        case value: String => value
      }
    } yield CloudLocation(bucket, prefix)

  /**
    * Reads the offset watermark from the given source root and offset map.
    *
    * @param sourceRoot The root CloudLocation object.
    * @param offsetMap  A map containing offset information.
    * @return An Option containing the CommitWatermark if the conversion is successful, None otherwise.
    */
  def readOffsetWatermark(
    sourceRoot: CloudLocation,
    offsetMap:  Map[String, _],
  ): Option[CommitWatermark] =
    for {
      cloudLocation <- mapToOffset(sourceRoot, offsetMap)
      lastLineBool <- offsetMap.get(LastLine).collect {
        case value: String => ContextBoolean.stringToBoolean(value).toOption
      }.flatten
    } yield {
      CommitWatermark(
        cloudLocation,
        lastLineBool,
      )
    }

  /**
    * Converts a map of offset information to a CloudLocation object.
    *
    * @param sourceRoot The root CloudLocation object.
    * @param offsetMap  A map containing offset information.
    * @return An Option containing the updated CloudLocation object if the conversion is successful, None otherwise.
    */
  def mapToOffset(
    sourceRoot: CloudLocation,
    offsetMap:  Map[String, _],
  ): Option[CloudLocation] =
    for {
      path <- offsetMap.get(PathKey).collect { case value: String => value }
      line <- offsetMap.get(LineKey).collect { case value: String if value forall Character.isDigit => value.toInt }
      ts = offsetMap.get(TimeStampKey).collect {
        case value: String if value forall Character.isDigit => Instant.ofEpochMilli(value.toLong)
      }
    } yield {
      sourceRoot.copy(
        path      = path.some,
        line      = line.some,
        timestamp = ts,
      )(sourceRoot.cloudLocationValidator)
    }

  def convertWatermarkToHeaders(
    writeWatermarkToHeaders: Boolean,
    partition:               util.Map[String, String],
    offset:                  util.Map[String, String],
  ) =
    Option.when(writeWatermarkToHeaders) {
      val newHeaders = new ConnectHeaders()
      (offset.asScala ++ partition.asScala).foreach {
        case (k, v) => newHeaders.addString(k, v)
      }
      newHeaders
    }
}
