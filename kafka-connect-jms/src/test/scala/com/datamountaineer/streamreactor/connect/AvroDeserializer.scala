package com.datamountaineer.streamreactor.connect

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

object AvroDeserializer {
  def apply(data: Array[Byte], schema: Schema): GenericRecord = {
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(data, null)
    reader.read(null, decoder)
  }
}

