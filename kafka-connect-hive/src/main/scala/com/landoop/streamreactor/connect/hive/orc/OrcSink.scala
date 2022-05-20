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
