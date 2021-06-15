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

package io.lenses.streamreactor.connect.aws.s3.model

import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.LOCAL_TMP_DIRECTORY
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model.S3WriteMode.{BuildLocal, Streamed}
import io.lenses.streamreactor.connect.aws.s3.storage.{BuildLocalOutputStream, MultipartBlobStoreOutputStream, S3OutputStream, StorageInterface}

import java.nio.file.Files
import java.util.UUID


sealed trait S3OutputStreamOptions {
  def createFormatWriter(formatSelection: FormatSelection, path: RemotePathLocation)(implicit storageInterface: StorageInterface): S3FormatWriter
}

object S3OutputStreamOptions extends LazyLogging{

  def apply(writeMode: String, props: Map[String, String]) : Either[Exception, S3OutputStreamOptions] = {
    S3WriteMode.withNameInsensitiveOption(writeMode) match {
      case Some(BuildLocal) => BuildLocalOutputStreamOptions(props)
      case Some(Streamed) => StreamedWriteOutputStreamOptions().asRight[Exception]
      case None => logger.warn(s"Unknown write mode ('$writeMode') requested, defaulting to 'Streamed'")
        StreamedWriteOutputStreamOptions().asRight[Exception]
    }
  }

}

case class StreamedWriteOutputStreamOptions() extends S3OutputStreamOptions {

  val MinAllowedMultipartSize: Int = 5242880

  override def createFormatWriter(formatSelection: FormatSelection, path: RemotePathLocation)(implicit storageInterface: StorageInterface): S3FormatWriter = {
    val streamFn = createOutputStreamFn(storageInterface, path)
    S3FormatWriter(formatSelection, streamFn)
  }

  private def createOutputStreamFn(storageInterface: StorageInterface, location: RemotePathLocation): () => S3OutputStream = {
    () => new MultipartBlobStoreOutputStream(location, MinAllowedMultipartSize)(storageInterface)
  }

}



object BuildLocalOutputStreamOptions {

  val PROPERTY_SINK_NAME = "name"

  def apply(props: Map[String, String]): Either[Exception, BuildLocalOutputStreamOptions]  = {

    def fetchFromProps(propertyToFetch: String) : Option[String] = {
      props
        .get(propertyToFetch)
        .filter(_.trim.nonEmpty)
    }

    fetchFromProps(LOCAL_TMP_DIRECTORY)
      .orElse(
        fetchFromProps(PROPERTY_SINK_NAME)
          .map(
            sinkName =>
              Option(Files.createTempDirectory(s"$sinkName.${UUID.randomUUID().toString}.").toAbsolutePath.toString)
          ).getOrElse(Option.empty[String])
      )
    match {
      case Some(value) => BuildLocalOutputStreamOptions(LocalLocation(value)).asRight[Exception]
      case None => new IllegalStateException(s"Either a local temporary directory ($LOCAL_TMP_DIRECTORY) or a Sink Name ($PROPERTY_SINK_NAME) must be configured to use '${BuildLocal.entryName}' write mode.").asLeft[BuildLocalOutputStreamOptions]
    }
  }
}

case class BuildLocalOutputStreamOptions(localLocation: LocalLocation) extends S3OutputStreamOptions {

  override def createFormatWriter(formatSelection: FormatSelection, path: RemotePathLocation)(implicit storageInterface: StorageInterface): S3FormatWriter = {
    val streamFn = createOutputStreamFn(storageInterface, LocalLocation(localLocation, path))
    S3FormatWriter(formatSelection, streamFn)
  }

  private def createOutputStreamFn(storageInterface: StorageInterface, location: LocalLocation): () => S3OutputStream = {
    () => new BuildLocalOutputStream(location)(storageInterface)
  }

}

