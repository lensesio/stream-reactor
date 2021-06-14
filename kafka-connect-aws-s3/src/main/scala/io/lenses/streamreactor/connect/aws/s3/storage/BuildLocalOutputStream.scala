
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

package io.lenses.streamreactor.connect.aws.s3.storage

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.{RemotePathLocation, LocalLocation}
import org.jclouds.blobstore.domain.{MultipartPart, MultipartUpload}

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.nio.ByteBuffer


class BuildLocalOutputStream(
                                      initialName: LocalLocation,
                                      cleanUp: Boolean = true,
                                    )(
                                      implicit storageInterface: StorageInterface
                                    ) extends OutputStream with LazyLogging with S3OutputStream {

  private val file = new File(initialName.path)

  private val outputStream = new BufferedOutputStream(new FileOutputStream(file))

  private var pointer = 0

  override def write(bytes: Array[Byte], startOffset: Int, numberOfBytes: Int): Unit = {

    require(bytes != null && bytes.nonEmpty, "Bytes must be provided")
    val endOffset = startOffset + numberOfBytes
    require(
      validateRange(startOffset, bytes.length) &&
        numberOfBytes > 0 &&
        validateRange(endOffset, bytes.length)
    )

    outputStream.write(bytes.slice(startOffset, endOffset))
    pointer += endOffset - startOffset
  }

  override def write(b: Int): Unit = {
    outputStream.write(b)
    pointer += 1
  }

  override def complete(finalDestination: RemotePathLocation): Unit = {
    outputStream.close()
    storageInterface.uploadFile(initialName, finalDestination)
    if (cleanUp) file.delete()
  }

  private def validateRange(startOffset: Int, numberOfBytes: Int) = startOffset >= 0 && startOffset <= numberOfBytes

  override def getPointer: Long = pointer
}
