/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.landoop.streamreactor.connect.hive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.ParquetWriter

package object parquet {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  def listFiles(path: Path)(implicit fs: FileSystem): List[Path] =
    if (fs.getFileStatus(path).isDirectory) {
      logger.debug(s"$path is a directory, reading constituent files")
      val remote = fs.listFiles(path, false)
      new Iterator[Path] {
        override def hasNext: Boolean = remote.hasNext
        override def next():  Path    = remote.next().getPath
      }.toList
    } else {
      logger.debug(s"Reading $path as a single file")
      List(path)
    }

  def parquetReader(file: Path)(implicit fs: FileSystem): ParquetReader[Struct] =
    ParquetReader.builder(new StructReadSupport, file)
      .withConf(fs.getConf)
      .build()

  def parquetWriter(path: Path, schema: Schema, config: ParquetSinkConfig): ParquetWriter[Struct] =
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
