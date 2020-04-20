package io.lenses.streamreactor.connect.aws.s3.formats

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import scala.collection.JavaConverters._


class AvroFormatReader extends Using {


  def read(bytes: Array[Byte]): List[GenericRecord] = {

    val datumReader = new GenericDatumReader[GenericRecord]()
    using(new SeekableByteArrayInput(bytes)) {
      inputStream =>
        using(new DataFileReader[GenericRecord](inputStream, datumReader)) {
          dataFileReader =>
            dataFileReader.iterator().asScala.toList

        }
    }

  }



}
