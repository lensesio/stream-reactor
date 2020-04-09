package com.datamountaineer.streamreactor.connect.influx

import java.io.StringWriter

import org.apache.avro.generic.GenericData

import java.io.ByteArrayOutputStream
import org.apache.avro.io.EncoderFactory

object AvroConverterUtil {

  def convertAvroToJsonString(msg: GenericData.Record): String = {
    val stringWriter = new StringWriter()

    val out = new ByteArrayOutputStream
    val encoder = EncoderFactory.get.jsonEncoder(msg.getSchema, out)
    encoder.flush()
    stringWriter.write(out.toString("UTF-8"))
    stringWriter.toString
  }
}
