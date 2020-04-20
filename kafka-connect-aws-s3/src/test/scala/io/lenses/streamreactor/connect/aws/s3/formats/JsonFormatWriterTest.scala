package io.lenses.streamreactor.connect.aws.s3.formats

import java.io.ByteArrayOutputStream

import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.storage.S3ByteArrayOutputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JsonFormatWriterTest extends AnyFlatSpec with Matchers {


  "convert" should "write byteoutputstream with json for a single record" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(() => outputStream)
    jsonFormatWriter.write(users(0), topic)

    outputStream.toString should be(recordsAsJson.head + "\n")

  }

  "convert" should "write byteoutputstream with json for multiple records" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new JsonFormatWriter(() => outputStream)
    users.foreach(jsonFormatWriter.write(_, topic))

    outputStream.toString should be(recordsAsJson.mkString("\n"))

  }
}
