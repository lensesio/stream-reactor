package io.lenses.streamreactor.connect.aws.s3.formats

import java.io.OutputStream
import java.nio.charset.StandardCharsets

import io.lenses.streamreactor.connect.aws.s3.Topic
import io.lenses.streamreactor.connect.aws.s3.storage.{MultipartBlobStoreOutputStream, S3OutputStream}
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.json.JsonConverter

import scala.collection.JavaConverters._

class JsonFormatWriter(outputStreamFn : () => S3OutputStream) extends S3FormatWriter {

  private val LineSeparatorBytes: Array[Byte] = System.lineSeparator.getBytes(StandardCharsets.UTF_8)

  private val outputStream: S3OutputStream = outputStreamFn()
  private val jsonConverter = new JsonConverter
  private var outstandingRename: Boolean = false

  jsonConverter.configure(
    Map("schemas.enable" -> false).asJava, false
  )

  override def write(struct: Struct, topic: Topic): Unit = {

    val dataBytes = jsonConverter.fromConnectData(topic.value, struct.schema(), struct)

    outputStream.write(dataBytes)
    outputStream.write(LineSeparatorBytes)
    outputStream.flush()
  }

  override def rolloverFileOnSchemaChange(): Boolean = false

  override def close: Unit = {
    outstandingRename = outputStream.complete()

    outputStream.flush()
    outputStream.close()
  }

  override def getOutstandingRename: Boolean = outstandingRename

  override def getPointer: Long = outputStream.getPointer()
}
