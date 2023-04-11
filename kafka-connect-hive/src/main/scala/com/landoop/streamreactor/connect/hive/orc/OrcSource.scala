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

import com.landoop.streamreactor.connect.hive.OrcSourceConfig
import com.landoop.streamreactor.connect.hive.kerberos.UgiExecute
import com.landoop.streamreactor.connect.hive.orc.vectors.OrcVectorReader.fromSchema
import com.landoop.streamreactor.connect.hive.orc.vectors.StructVectorReader
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.kafka.connect.data.Struct
import org.apache.orc.OrcFile.ReaderOptions
import org.apache.orc.OrcFile
import org.apache.orc.Reader

import scala.jdk.CollectionConverters.ListHasAsScala

class OrcSource(path: Path, config: OrcSourceConfig, ugi: UgiExecute)(implicit fs: FileSystem) extends StrictLogging {

  private val reader = OrcFile.createReader(path, new ReaderOptions(fs.getConf))

  private val typeDescription = reader.getSchema

  private val readers      = typeDescription.getChildren.asScala.map(fromSchema)
  private val vectorReader = new StructVectorReader(readers.toIndexedSeq, typeDescription)

  private val batch        = typeDescription.createRowBatch()
  private val recordReader = reader.rows(new Reader.Options())

  def close(): Unit =
    recordReader.close()

  def iterator: Iterator[Struct] = new Iterator[Struct] {
    var iter = new BatchIterator(batch)
    override def hasNext: Boolean = ugi.execute {
      iter.hasNext || {
        batch.reset()
        recordReader.nextBatch(batch)
        iter = new BatchIterator(batch)
        !batch.endOfFile && batch.size > 0 && iter.hasNext
      }
    }
    override def next(): Struct = ugi.execute {
      iter.next()
    }
  }

  // iterates over a batch, be careful not to mutate the batch while it is being iterated
  class BatchIterator(batch: VectorizedRowBatch) extends Iterator[Struct] {
    var offset = 0
    val vector = new StructColumnVector(batch.numCols, batch.cols: _*)
    override def hasNext: Boolean = offset < batch.size
    override def next(): Struct = {
      val struct = vectorReader.read(offset, vector)
      offset = offset + 1
      struct.orNull
    }
  }

}
