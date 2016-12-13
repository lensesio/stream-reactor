package com.datamountaineer.streamreactor.connect.mqtt.source

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory

object AvroSerializer {
  def apply(record: GenericRecord, schema: Schema) = {
    val output = new ByteArrayOutputStream()
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val encoder = EncoderFactory.get().binaryEncoder(output, null)

    writer.write(record, encoder)
    encoder.flush()
    output.flush()
    output.toByteArray
  }
}
