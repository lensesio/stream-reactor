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
package io.lenses.streamreactor.connect.cloud.common.consumers

import java.nio.ByteBuffer

/**
  * Mimics the Kafka core OffsetAndMetadata class.
  */

case class OffsetAndMetadata(
  offset:          Long,
  leaderEpoch:     Int,
  metadata:        String,
  commitTimestamp: Long,
  expireTimestamp: Long,
)

object OffsetAndMetadata {
  private val LOWEST_SUPPORTED_VERSION:  Short = 0
  private val HIGHEST_SUPPORTED_VERSION: Short = 3

  def from(buffer: ByteBuffer): Either[Throwable, OffsetAndMetadata] =
    Option(buffer).toRight(new IllegalArgumentException("Buffer cannot be null")).flatMap { _ =>
      val version = buffer.getShort()
      if (version >= LOWEST_SUPPORTED_VERSION && version <= HIGHEST_SUPPORTED_VERSION) {
        val offset         = buffer.getLong()
        val leaderEpoch    = if (version >= 3) buffer.getInt() else -1
        val metadataLength = buffer.getShort()
        val metadataBytes  = new Array[Byte](metadataLength.toInt)
        buffer.get(metadataBytes)
        val metadata        = new String(metadataBytes)
        val commitTimestamp = buffer.getLong()
        val expireTimestamp = if (version == 1) buffer.getLong() else -1L
        Right(OffsetAndMetadata(offset, leaderEpoch, metadata, commitTimestamp, expireTimestamp))
      } else {
        Left(new IllegalArgumentException(s"Unknown offset message version: $version"))
      }
    }
}
