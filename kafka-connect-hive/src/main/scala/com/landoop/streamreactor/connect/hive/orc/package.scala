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

import com.landoop.streamreactor.connect.hive.kerberos.UgiExecute
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.kafka.connect.data.Schema
import org.apache.orc.OrcFile.EncodingStrategy
import org.apache.orc._

package object orc {

  def createOrcWriter(path: Path, schema: TypeDescription, config: OrcSinkConfig)(implicit fs: FileSystem): Writer = {

    val options = OrcFile.writerOptions(null, fs.getConf).setSchema(schema)

    options.compress(config.compressionKind)
    options.encodingStrategy(config.encodingStrategy)
    options.blockPadding(config.blockPadding)
    options.version(OrcFile.Version.V_0_12)

    config.bloomFilterColumns.map(_.mkString(",")).foreach(options.bloomFilterColumns)
    config.rowIndexStride.foreach(options.rowIndexStride)
    config.blockSize.foreach(options.blockSize)
    config.stripeSize.foreach(options.stripeSize)

    if (config.overwrite && fs.exists(path))
      fs.delete(path, false)

    OrcFile.createWriter(path, options)
  }

  def source(path: Path, config: OrcSourceConfig, ugi: UgiExecute)(implicit fs: FileSystem) =
    new OrcSource(path, config, ugi)

  def sink(path: Path, schema: Schema, config: OrcSinkConfig)(implicit fs: FileSystem) =
    new OrcSink(path, schema, config)
}

case class OrcSourceConfig()

case class OrcSinkConfig(
  overwrite:          Boolean          = false,
  batchSize:          Int              = 1024, // orc default is 1024
  encodingStrategy:   EncodingStrategy = EncodingStrategy.COMPRESSION,
  compressionKind:    CompressionKind  = CompressionKind.SNAPPY,
  blockPadding:       Boolean          = true,
  blockSize:          Option[Long]     = None,
  stripeSize:         Option[Long]     = None,
  bloomFilterColumns: Seq[String]      = Nil,
  rowIndexStride:     Option[Int]      = None,
)
