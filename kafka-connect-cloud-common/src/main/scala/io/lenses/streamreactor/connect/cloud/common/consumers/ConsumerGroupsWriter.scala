/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.consumers

import cats.implicits.toShow
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.consumers.ConsumerGroupsWriter.extractOffsets
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import java.nio.ByteBuffer

class ConsumerGroupsWriter(location: CloudObjectKey, uploader: Uploader, taskId: ConnectorTaskId)
    extends AutoCloseable
    with StrictLogging {

  override def close(): Unit = uploader.close()

  def write(records: List[SinkRecord]): Either[Throwable, Unit] =
    records.traverse(extractOffsets)
      .map(_.flatten)
      .map { offsets =>
        offsets.foldLeft(Map.empty[GroupTopicPartition, OffsetAction]) {
          case (acc, action: OffsetAction) =>
            acc + (action.key -> action)
        }
      }.flatMap { map =>
        map.toList.traverse {
          case (groupTopicPartition, action) =>
            val s3KeySuffix =
              s"${groupTopicPartition.group}/${groupTopicPartition.topic}/${groupTopicPartition.partition}"
            val s3Key = location.prefix.fold(s3KeySuffix)(prefix => s"$prefix/$s3KeySuffix")

            action match {
              case WriteOffset(offset) =>
                val content = ByteBuffer.allocate(8).putLong(offset.metadata.offset).rewind()
                logger.debug(s"[${taskId.show}] Uploading offset $offset to $s3Key")
                val result = uploader.upload(
                  content,
                  location.bucket,
                  s3Key,
                )
                logger.debug(s"[${taskId.show}] Uploaded offset $offset to $s3Key")
                result
              case DeleteOffset(_) =>
                logger.debug(s"[${taskId.show}] Deleting offset $s3Key")
                val result = uploader.delete(
                  location.bucket,
                  s3Key,
                )
                logger.debug(s"[${taskId.show}] Deleted offset $s3Key")
                result
            }
        }.map(_ => ())
      }
}

object ConsumerGroupsWriter extends StrictLogging {

  /**
    * Expects the [[SinkRecord]] to contain the key and value as byte arrays.
    * The key is expected to be a byte array representation of an [[OffsetKey]], and ignores GroupMetadata keys.
    * @param record The [[SinkRecord]] to extract the offset details from.
    * @return Either an error or the offset details.
    */
  def extractOffsets(record: SinkRecord): Either[Throwable, Option[OffsetAction]] =
    Option(record.key()) match {
      case None => Right(None)
      case Some(key) =>
        for {
          keyBytes <- validateByteArray(key,
                                        "key",
                                        "key.converter=org.apache.kafka.connect.converters.ByteArrayConverter",
          )
          buffer  = ByteBuffer.wrap(keyBytes)
          version = buffer.getShort
          result <- if (version == 0 || version == 1) {
            for {
              key <- OffsetKey.from(version, buffer)
              value <- Option(record.value()) match {
                case Some(value) =>
                  for {
                    valueBytes <- validateByteArray(
                      value,
                      "value",
                      "value.converter=org.apache.kafka.connect.converters.ByteArrayConverter",
                    )
                    metadata <- OffsetAndMetadata.from(ByteBuffer.wrap(valueBytes))
                  } yield {
                    Some(WriteOffset(OffsetDetails(key, metadata)))
                  }
                case None =>
                  Right(Some(DeleteOffset(key.key)))
              }
            } yield value
          } else {
            Right(None)
          }
        } yield result
    }

  private def validateByteArray(value: Any, name: String, converter: String): Either[Throwable, Array[Byte]] =
    value match {
      case bytes: Array[Byte] => Right(bytes)
      case _ =>
        Left(
          new ConnectException(
            s"The record $name is not a byte array. Make sure the connector configuration uses '$converter'.",
          ),
        )
    }
}

case class OffsetDetails(key: OffsetKey, metadata: OffsetAndMetadata)

sealed trait OffsetAction {
  def key: GroupTopicPartition
}

case class WriteOffset(offset: OffsetDetails) extends OffsetAction {
  override def key: GroupTopicPartition = offset.key.key
}
case class DeleteOffset(key: GroupTopicPartition) extends OffsetAction
