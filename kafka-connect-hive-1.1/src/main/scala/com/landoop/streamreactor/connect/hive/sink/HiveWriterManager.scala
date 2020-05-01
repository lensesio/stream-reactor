package com.landoop.streamreactor.connect.hive.sink

import com.landoop.streamreactor.connect.hive.{Offset, TopicPartition, TopicPartitionOffset}
import com.landoop.streamreactor.connect.hive.formats.{HiveFormat, HiveWriter}
import com.landoop.streamreactor.connect.hive.sink.staging.StageManager
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.connect.data.Schema

/**
 * Manages the lifecycle of [[HiveWriter]] instances.
 *
 * A given sink may be writing to multiple locations (partitions), and therefore
 * it is convenient to extract this to another class.
 *
 * This class is not thread safe as it is not designed to be shared between concurrent
 * sinks, since file handles cannot be safely shared without considerable overhead.
 */
class HiveWriterManager(format: HiveFormat,
                        stageManager: StageManager)
                       (implicit fs: FileSystem) {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  case class WriterKey(tp: TopicPartition, dir: Path)

  case class OpenWriter(tp: TopicPartition, dir: Path, writer: HiveWriter)

  private val writers = scala.collection.mutable.Map.empty[WriterKey, HiveWriter]

  private def createWriter(dir: Path,
                           tp: TopicPartition,
                           schema: Schema): HiveWriter = {
    val path = stageManager.stage(dir, tp)
    logger.debug(s"Staging new writer at path [$path]")
    format.writer(path, schema)
  }

  /**
   * Returns a writer that can write records for a particular topic and partition.
   * The writer will create a file inside the given directory if there is no open writer.
   */
  def writer(dir: Path, tp: TopicPartition, schema: Schema): HiveWriter = {
    writers.getOrElseUpdate(WriterKey(tp, dir), createWriter(dir, tp, schema))
  }

  /**
   * Flushes the open writer for the given topic partition and directory.
   *
   * Next time a writer is requested for the given (topic,partition,directory), a new
   * writer will be created.
   *
   * The directory is required, as there may be multiple writers, one per partition.
   * The offset is required as part of the commit filename.
   */
  def flush(tpo: TopicPartitionOffset, dir: Path): Unit = {
    logger.info(s"Flushing writer for $tpo")
    val key = WriterKey(tpo.toTopicPartition, dir)
    writers.get(key).foreach { writer =>
      writer.close()
      stageManager.commit(writer.file, tpo)
      writers.remove(key)
    }
  }

  /**
   * Flushes all open writers.
   *
   * @param offsets the offset for each [[TopicPartition]] which is required
   *                by the commit process.
   */
  def flush(offsets: Map[TopicPartition, Offset]): Unit = {
    logger.info(s"Flushing offsets $offsets")
    // we may not have an offset for a given topic/partition if no data was written to that TP
    writers.foreach { case (key, writer) =>
      writer.close()
      offsets.get(key.tp).foreach { offset =>
        stageManager.commit(writer.file, key.tp.withOffset(offset))
      }
      writers.remove(key)
    }
  }

  def getWriters: Seq[OpenWriter] = writers.map { case (key, writer) => OpenWriter(key.tp, key.dir, writer) }.toList
}