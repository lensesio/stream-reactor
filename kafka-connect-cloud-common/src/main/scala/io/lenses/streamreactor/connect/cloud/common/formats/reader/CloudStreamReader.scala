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
package io.lenses.streamreactor.connect.cloud.common.formats.reader

import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import org.apache.kafka.connect.source.SourceRecord

trait CloudStreamReader extends AutoCloseable with Iterator[SourceRecord] {

  def getBucketAndPath: CloudLocation

  def currentRecordIndex: Long
}

class EmptyCloudStreamReader(location: CloudLocation) extends CloudStreamReader {
  override def getBucketAndPath: CloudLocation = location

  override def currentRecordIndex: Long = -1L

  override def hasNext: Boolean = false

  override def next(): SourceRecord = throw new UnsupportedOperationException("EmptyCloudStreamReader")

  override def close(): Unit = ()
}

trait CloudDataIterator[T] extends Iterator[T] with AutoCloseable

trait Converter[T] {
  def convert(t: T, index: Long, lastLine: Boolean): SourceRecord
}

class DelegateIteratorCloudStreamReader[T](
  iterator:  CloudDataIterator[T],
  converter: Converter[T],
  location:  CloudLocation,
) extends CloudStreamReader {

  // It starts at -1 to signal nothing read. However, this means 0 is the first record.
  // The connector watermark needs to take this into account.
  // It would have been an option to change to 0 as no-records,
  // but this means the current connectors will skip a record once updated.
  // So now this inconsistency is here to stay.
  private var recordIndex:       Long          = -1
  override def getBucketAndPath: CloudLocation = location

  override def currentRecordIndex: Long = recordIndex

  override def hasNext: Boolean = iterator.hasNext

  override def next(): SourceRecord = {
    val data = iterator.next()
    recordIndex = recordIndex + 1
    converter.convert(data, recordIndex, !hasNext)
  }

  override def close(): Unit = iterator.close()
}
