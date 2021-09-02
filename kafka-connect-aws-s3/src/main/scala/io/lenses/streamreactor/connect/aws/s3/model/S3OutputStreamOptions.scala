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
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.LOCAL_TMP_DIRECTORY
import io.lenses.streamreactor.connect.aws.s3.config.{FormatSelection, S3ConfigDefBuilder}
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model.S3WriteMode.{BuildLocal, Streamed}
import io.lenses.streamreactor.connect.aws.s3.model.location.{LocalPathLocation, LocalRootLocation, RemoteS3PathLocation}
import io.lenses.streamreactor.connect.aws.s3.processing.BlockingQueueProcessor
import io.lenses.streamreactor.connect.aws.s3.sink.ProcessorException
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import io.lenses.streamreactor.connect.aws.s3.storage.stream.{BuildLocalOutputStream, MultipartBlobStoreOutputStream, S3OutputStream}

import java.nio.file.Files
import java.util.UUID
import scala.util.{Failure, Success, Try}


sealed trait S3OutputStreamOptions {
  def createFormatWriter(formatSelection: FormatSelection, path: RemoteS3PathLocation, initialOffset: Offset, updateOffsetFn: Offset => () => Unit)(implicit storageInterface: StorageInterface, queueProcessor: BlockingQueueProcessor): Either[ProcessorException, S3FormatWriter]
}

object S3OutputStreamOptions extends LazyLogging {

  def apply(writeMode: String, s3ConfigDefBuilder: S3ConfigDefBuilder): Either[Exception, S3OutputStreamOptions] = {
    S3WriteMode.withNameInsensitiveOption(writeMode) match {
      case Some(BuildLocal) => BuildLocalOutputStreamOptions(s3ConfigDefBuilder)
      case Some(Streamed) => StreamedWriteOutputStreamOptions().asRight[Exception]
      case None => logger.warn(s"Unknown write mode ('$writeMode') requested, defaulting to 'Streamed'")
        StreamedWriteOutputStreamOptions().asRight[Exception]
    }
  }

}

case class StreamedWriteOutputStreamOptions() extends S3OutputStreamOptions {

  override def createFormatWriter(formatSelection: FormatSelection, path: RemoteS3PathLocation, initialOffset: Offset, updateOffsetFn: Offset => () => Unit)(implicit storageInterface: StorageInterface, queueProcessor: BlockingQueueProcessor): Either[ProcessorException, S3FormatWriter] = {
    Try(createOutputStreamFn(path, initialOffset, updateOffsetFn)) match {
      case Failure(exception: Throwable) => ProcessorException(exception).asLeft
      case Success(streamFn) => S3FormatWriter(formatSelection, streamFn).asRight
    }
  }

  private def createOutputStreamFn(location: RemoteS3PathLocation, initialOffset: Offset, updateOffsetFn: Offset => () => Unit)(implicit queueProcessor: BlockingQueueProcessor): () => S3OutputStream = {
    () => new MultipartBlobStoreOutputStream(location, initialOffset, updateOffsetFn)
  }

}


object BuildLocalOutputStreamOptions {

  val PROPERTY_SINK_NAME = "name"

  def apply(s3ConfigDefBuilder: S3ConfigDefBuilder): Either[Exception, BuildLocalOutputStreamOptions] = {

    val sinkName = s3ConfigDefBuilder.sinkName
    val props = s3ConfigDefBuilder.getParsedValues

    def fetchFromProps(propertyToFetch: String): Option[String] = {
      props
        .get(propertyToFetch)
      match {
        case Some(value: String) if value.trim.nonEmpty => Some(value.trim)
        case Some(_) => None
        case None => None
      }

    }

    fetchFromProps(LOCAL_TMP_DIRECTORY)
      .orElse(
        sinkName
          .map(
            sinkName =>
              Option(Files.createTempDirectory(s"$sinkName.${UUID.randomUUID().toString}.").toAbsolutePath.toString)
          ).getOrElse(Option.empty[String])
      )
    match {
      case Some(value) => BuildLocalOutputStreamOptions(LocalRootLocation(value)).asRight[Exception]
      case None => new IllegalStateException(s"Either a local temporary directory ($LOCAL_TMP_DIRECTORY) or a Sink Name ($PROPERTY_SINK_NAME) must be configured to use '${BuildLocal.entryName}' write mode.").asLeft[BuildLocalOutputStreamOptions]
    }
  }
}

case class BuildLocalOutputStreamOptions(localLocation: LocalRootLocation) extends S3OutputStreamOptions {

  override def createFormatWriter(formatSelection: FormatSelection, path: RemoteS3PathLocation, initialOffset: Offset, updateOffsetFn: Offset => () => Unit)(implicit storageInterface: StorageInterface, processor: BlockingQueueProcessor): Either[ProcessorException, S3FormatWriter] = {
    Try(createOutputStreamFn(path.toLocalPathLocation(localLocation), initialOffset, updateOffsetFn)) match {
      case Failure(exception) => ProcessorException(exception).asLeft
      case Success(streamFn) => S3FormatWriter(formatSelection, streamFn).asRight
    }
  }

  private def createOutputStreamFn(location: LocalPathLocation, initialOffset: Offset, updateOffsetFn: Offset => () => Unit)(implicit queueProcessor: BlockingQueueProcessor): () => S3OutputStream = {
    () => new BuildLocalOutputStream(location, updateOffsetFn)
  }

}

