package com.landoop.streamreactor.connect.hive

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetReader, ParquetWriter}

package object parquet {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  def listFiles(path: Path)(implicit fs: FileSystem): List[Path] = {
    if (fs.isDirectory(path)) {
      logger.debug(s"$path is a directory, reading constituent files")
      val remote = fs.listFiles(path, false)
      new Iterator[Path] {
        override def hasNext: Boolean = remote.hasNext
        override def next(): Path = remote.next().getPath
      }.toList
    } else {
      logger.debug(s"Reading $path as a single file")
      List(path)
    }
  }

  def parquetReader(file: Path)(implicit fs: FileSystem): ParquetReader[Struct] = {
    ParquetReader.builder(new StructReadSupport, file)
      .withConf(fs.getConf)
      .build()
  }

  def parquetWriter(path: Path,
                    schema: Schema,
                    config: ParquetSinkConfig): ParquetWriter[Struct] = {
    new StructParquetWriterBuilder(path, schema)
      .withCompressionCodec(config.compressionCodec)
      .withDictionaryEncoding(config.enableDictionary)
      .withValidation(config.validation)
      .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
      .withWriteMode(if (config.overwrite) {
        ParquetFileWriter.Mode.OVERWRITE
      } else {
        ParquetFileWriter.Mode.CREATE
      }).build()
  }
}

