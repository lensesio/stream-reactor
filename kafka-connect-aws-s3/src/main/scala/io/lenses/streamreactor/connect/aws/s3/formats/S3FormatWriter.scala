
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

package io.lenses.streamreactor.connect.aws.s3.formats

import io.lenses.streamreactor.connect.aws.s3.Topic
import io.lenses.streamreactor.connect.aws.s3.config.Format
import io.lenses.streamreactor.connect.aws.s3.config.Format.{Avro, Json, Parquet}
import io.lenses.streamreactor.connect.aws.s3.storage.MultipartBlobStoreOutputStream
import org.apache.kafka.connect.data.Struct

object S3FormatWriter {

  def apply(
             format: Format,
             outputStreamFn : () => MultipartBlobStoreOutputStream
           ): S3FormatWriter = {

    format match {
      case Parquet => new ParquetFormatWriter(outputStreamFn)
      case Json => new JsonFormatWriter(outputStreamFn)
      case Avro => new AvroFormatWriter(outputStreamFn)
      case _ => sys.error(s"Unsupported S3 format $format")
    }
  }

}

trait S3FormatWriter extends AutoCloseable {

  def rolloverFileOnSchemaChange(): Boolean

  def write(struct: Struct, topic: Topic): Unit

  def getOutstandingRename: Boolean

  def getPointer: Long
}


