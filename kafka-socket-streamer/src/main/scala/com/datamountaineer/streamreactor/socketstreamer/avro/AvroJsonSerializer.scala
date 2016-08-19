package com.datamountaineer.streamreactor.socketstreamer.avro

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

import org.apache.avro.generic.{GenericContainer, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory


object AvroJsonSerializer {

  implicit class GenericRecordToJsonConverter(val record: GenericRecord) extends AnyVal {
    def toJson() = {
      val jsonOutputStream = new ByteArrayOutputStream()
      val jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema, jsonOutputStream)

      val writer = new GenericDatumWriter[GenericContainer]()
      writer.setSchema(record.getSchema)
      writer.write(record, jsonEncoder)
      val json = new String(jsonOutputStream.toByteArray, Charset.forName("UTF-8"))
      jsonOutputStream.close()
      json

    }
  }

}
