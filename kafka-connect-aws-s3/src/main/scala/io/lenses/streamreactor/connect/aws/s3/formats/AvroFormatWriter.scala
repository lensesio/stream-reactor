package io.lenses.streamreactor.connect.aws.s3.formats

import java.io.OutputStream

import com.typesafe.scalalogging.LazyLogging
import io.confluent.connect.avro.AvroData
import io.lenses.streamreactor.connect.aws.s3.Topic
import io.lenses.streamreactor.connect.aws.s3.storage.{MultipartBlobStoreOutputStream, S3OutputStream}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.kafka.connect.data.Struct

class AvroFormatWriter(outputStreamFn : () => S3OutputStream) extends S3FormatWriter with LazyLogging {

  private val avroDataConverter = new AvroData(100)

  private var outputStream: S3OutputStream = _
  private var writer: GenericDatumWriter[GenericRecord] = _
  private var fileWriter: DataFileWriter[GenericRecord] = _
  private var outstandingRename: Boolean = false

  override def write(struct: Struct, topic: Topic): Unit = {
    logger.debug("AvroFormatWriter - write")

    val genericRecord: GenericRecord = avroDataConverter.fromConnectData(struct.schema(), struct).asInstanceOf[GenericRecord]
    if (outputStream == null || fileWriter == null) {
      init(genericRecord.getSchema)
    }
    fileWriter.append(genericRecord)
    fileWriter.flush()
  }

  private def init(schema: Schema): Unit = {
    outputStream = outputStreamFn()
    writer = new GenericDatumWriter[GenericRecord](schema)
    fileWriter = new DataFileWriter[GenericRecord](writer).create(schema, outputStream)
  }

  override def rolloverFileOnSchemaChange() = true

  override def close: Unit = {
    fileWriter.flush()

    outstandingRename = outputStream.complete()

    fileWriter.close()
    outputStream.close()

  }

  override def getOutstandingRename: Boolean = outstandingRename

  override def getPointer: Long = outputStream.getPointer()
}
