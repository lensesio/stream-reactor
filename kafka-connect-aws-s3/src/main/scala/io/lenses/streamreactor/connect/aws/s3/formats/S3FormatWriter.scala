package io.lenses.streamreactor.connect.aws.s3.formats

import java.io.OutputStream

import io.lenses.streamreactor.connect.aws.s3.{BucketAndPath, Topic}
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


