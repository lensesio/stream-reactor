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
package com.landoop.streamreactor.connect.hive.orc

import com.landoop.streamreactor.connect.hive.orc.vectors.OrcVectorWriter
import com.landoop.streamreactor.connect.hive.orc.vectors.StructVectorWriter
import com.landoop.streamreactor.connect.hive.OrcSinkConfig
import com.landoop.streamreactor.connect.hive.StructUtils
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

import scala.jdk.CollectionConverters.ListHasAsScala

class OrcSink(path: Path, schema: Schema, config: OrcSinkConfig)(implicit fs: FileSystem) extends StrictLogging {

  private val typeDescription = OrcSchemas.toOrc(schema)
  private val structWriter =
    new StructVectorWriter(typeDescription.getChildren.asScala.map(OrcVectorWriter.fromSchema).toSeq)
  private val batch     = typeDescription.createRowBatch(config.batchSize)
  private val vector    = new StructColumnVector(batch.numCols, batch.cols: _*)
  private val orcWriter = createOrcWriter(path, typeDescription, config)
  private var n         = 0

  def flush(): Unit = {
    logger.debug(s"Writing orc batch [size=$n, path=$path]")
    batch.size = n
    orcWriter.addRowBatch(batch)
    orcWriter.writeIntermediateFooter
    batch.reset()
    n = 0
  }

  def write(struct: Struct): Unit = {
    structWriter.write(vector, n, Some(StructUtils.extractValues(struct)))
    n = n + 1
    if (n == config.batchSize)
      flush()
  }

  def close(): Unit = {
    if (n > 0)
      flush()
    orcWriter.close()
  }
}
