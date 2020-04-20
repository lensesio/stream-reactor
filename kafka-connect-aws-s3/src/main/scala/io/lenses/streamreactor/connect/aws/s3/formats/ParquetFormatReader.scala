package io.lenses.streamreactor.connect.aws.s3.formats

import io.lenses.streamreactor.connect.aws.s3.formats.parquet.{ParquetInputFile, SeekableByteArrayInputStream}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader

class ParquetFormatReader extends Using {
  def read(bytes: Array[Byte]): List[GenericRecord] = {

    using(new SeekableByteArrayInputStream(bytes)) {
      inputStream =>
        val inputFile = new ParquetInputFile(inputStream)

        using(AvroParquetReader
          .builder[GenericRecord](inputFile)
          .build()) {
          avroParquetReader =>
            accListFromReader(avroParquetReader, Vector())
              .toList
        }
    }
  }

  def accListFromReader[A](reader: ParquetReader[A], acc: Vector[A]): Vector[A] = {
    val current = reader.read()
    if (current == null) acc
    else accListFromReader(reader, acc :+ current)
  }
}
