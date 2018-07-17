package com.landoop.streamreactor.connect.hive.orc

import com.landoop.streamreactor.connect.hive.OrcSourceConfig
import com.landoop.streamreactor.connect.hive.orc.vectors.OrcVectorReader.fromSchema
import com.landoop.streamreactor.connect.hive.orc.vectors.StructVectorReader
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.exec.vector.{StructColumnVector, VectorizedRowBatch}
import org.apache.kafka.connect.data.Struct
import org.apache.orc.OrcFile
import org.apache.orc.OrcFile.ReaderOptions

import scala.collection.JavaConverters._

class OrcSource(path: Path, config: OrcSourceConfig)(implicit fs: FileSystem) extends StrictLogging {

  private val reader = OrcFile.createReader(path, new ReaderOptions(fs.getConf))

  private val typeDescription = reader.getSchema
  private val schema = OrcSchemas.toKafka(typeDescription)

  private val readers = typeDescription.getChildren.asScala.map(fromSchema)
  private val vectorReader = new StructVectorReader(readers.toIndexedSeq, typeDescription)

  private val batch = typeDescription.createRowBatch()
  private val recordReader = reader.rows(reader.options())

  def close(): Unit = {
    recordReader.close()
  }

  def iterator: Iterator[Struct] = new Iterator[Struct] {
    var iter = new BatchIterator(batch)
    override def hasNext: Boolean = iter.hasNext || {
      batch.reset()
      recordReader.nextBatch(batch)
      iter = new BatchIterator(batch)
      !batch.endOfFile && batch.size > 0 && iter.hasNext
    }
    override def next(): Struct = iter.next()
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
