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

import io.lenses.streamreactor.connect.aws.s3.model.location.{RemoteS3PathLocationWithLine, RemoteS3RootLocation}
import io.lenses.streamreactor.connect.aws.s3.source.SourceRecordConverter.fromSourcePartition
import org.apache.kafka.connect.source.SourceTaskContext

import scala.jdk.CollectionConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.util.Try

class ContextReader(context: () => SourceTaskContext) {

  def getCurrentOffset(sourceRoot: RemoteS3RootLocation): Option[RemoteS3PathLocationWithLine] = {
    val key = fromSourcePartition(sourceRoot).asJava
    for {
      offsetMap <- Try(context().offsetStorageReader.offset(key).asScala).toOption.filterNot(_ == null)
      path <- offsetMap.get("path").collect { case value: String => value }
      line <- offsetMap.get("line").collect { case value: String if value forall Character.isDigit => value.toInt }
    } yield sourceRoot.withPath(path).atLine(line)
  }

}
