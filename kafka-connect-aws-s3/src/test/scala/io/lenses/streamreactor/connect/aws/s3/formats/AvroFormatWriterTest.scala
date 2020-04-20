package io.lenses.streamreactor.connect.aws.s3.formats

import java.io.ByteArrayOutputStream

import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.storage.S3ByteArrayOutputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroFormatWriterTest extends AnyFlatSpec with Matchers {

  val avroFormatReader = new AvroFormatReader()

  "convert" should "write byteoutputstream with json for a single record" in {

    val outputStream = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(() => outputStream)
    avroFormatWriter.write(users(0), topic)
    avroFormatWriter.close()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)

    genericRecords.size should be(1)
    checkRecord(genericRecords(0), "sam", "mr", 100.43)
  }

  "convert" should "write byteoutputstream with json for multiple records" in {

    val outputStream = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(() => outputStream)
    users.foreach(avroFormatWriter.write(_, topic))
    avroFormatWriter.close()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)
    genericRecords.size should be(3)
  }
}
