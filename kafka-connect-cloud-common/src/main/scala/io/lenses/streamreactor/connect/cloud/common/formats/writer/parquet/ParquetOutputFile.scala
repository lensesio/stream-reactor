/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.formats.writer.parquet

import io.lenses.streamreactor.connect.cloud.common.stream.CloudOutputStream
import org.apache.parquet.io.OutputFile
import org.apache.parquet.io.PositionOutputStream

class ParquetOutputFile(multipartBlobStoreOutputStream: CloudOutputStream) extends OutputFile {

  override def create(blockSizeHint: Long): PositionOutputStream =
    new PositionOutputStreamWrapper(multipartBlobStoreOutputStream)

  override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream =
    new PositionOutputStreamWrapper(multipartBlobStoreOutputStream)

  override def supportsBlockSize(): Boolean = false

  override def defaultBlockSize(): Long = 0

  class PositionOutputStreamWrapper(multipartBlobStoreOutputStream: CloudOutputStream) extends PositionOutputStream {

    override def getPos: Long = multipartBlobStoreOutputStream.getPointer

    override def write(b: Int): Unit = multipartBlobStoreOutputStream.write(b)

  }

}
