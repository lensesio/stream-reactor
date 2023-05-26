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

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.ContextConstants.LineKey
import io.lenses.streamreactor.connect.aws.s3.source.ContextConstants.PathKey
import io.lenses.streamreactor.connect.aws.s3.source.ContextConstants.TimeStampKey
import io.lenses.streamreactor.connect.aws.s3.source.SourceRecordConverter.fromSourcePartition
import org.apache.kafka.connect.source.SourceTaskContext

import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Try

object SourceContextReader {

  def getCurrentOffset(
    context:    () => SourceTaskContext,
  )(sourceRoot: S3Location,
  ): Option[S3Location] = {
    val key = fromSourcePartition(sourceRoot).asJava
    for {
      offsetMap <- Try(context().offsetStorageReader.offset(key).asScala).toOption.filterNot(_ == null)
      path      <- offsetMap.get(PathKey).collect { case value: String => value }
      line      <- offsetMap.get(LineKey).collect { case value: String if value forall Character.isDigit => value.toInt }
      ts = offsetMap.get(TimeStampKey).collect {
        case value: String if value forall Character.isDigit => Instant.ofEpochMilli(value.toLong)
      }
    } yield {
      sourceRoot.copy(
        path      = path.some,
        line      = line.some,
        timestamp = ts,
      )
    }
  }

}
