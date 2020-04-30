package com.landoop.streamreactor.connect.hive.sink

import com.landoop.streamreactor.connect.hive
import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.formats.HiveWriter
import com.landoop.streamreactor.connect.hive.sink.config.{HiveSinkConfig, TableOptions}
import com.landoop.streamreactor.connect.hive.sink.partitioning.CachedPartitionHandler
import com.landoop.streamreactor.connect.hive.sink.staging.{CommitContext, CommitPolicy, StageManager}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException

import scala.util.{Failure, Success}

/**
 * A [[HiveSink]] handles writing records to a single hive table.
 *
 * It will manage a set of [[HiveWriter]] instances, each of which will handle records in a partition.
 * is not partitioned, then this will be a single file.
 *
 * The hive sink uses a [[CommitPolicy]] to determine when to close a file and start
 * a new file. When the file is due to be commited, the [[StageManager]]
 * will be invoked to commit the file.
 *
 * The lifecycle of a sink is managed independently from other sinks.
 *
 * @param tableConfig the name of the table to write out to
 */
class HiveSink(tableConfig: TableOptions,
               stageManager: StageManager,
               dbName: DatabaseName)
              (implicit client: IMetaStoreClient, fs: FileSystem) {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private val writerManager = new HiveWriterManager(tableConfig.format, stageManager)
  private val partitioner = new CachedPartitionHandler(tableConfig.partitioner)

  private var state: HiveSinkState = _

  /**
   * Returns the appropriate output directory for the given struct.
   *
   * If the table is not partitioned, then the directory returned
   * will be the location of the table itself, otherwise it will delegate to
   * the partitioning policy.
   */
  private def outputDir(struct: Struct, plan: Option[PartitionPlan]): Path = {
    plan.fold(state.tableLocation) { plan =>
      val part = hive.partition(struct, plan)
      partitioner.path(part, dbName, tableConfig.tableName)(client, fs) match {
        case Failure(t) => throw t
        case Success(path) => path
      }
    }
  }

  def write(struct: Struct, tpo: TopicPartitionOffset): Unit = {

    // if the schema has changed, or if we haven't yet had a struct, we need
    // to make sure the table exists, and that our table metadata is up to date
    // given that the table may have evolved.
    if (state == null) {
      state = HiveSinkState.from(struct.schema, tableConfig, dbName)
    }
    else if (struct.schema != state.lastSchema) {
      state = state.withLastSchema(struct.schema)
    }

    val dir = outputDir(struct, state.plan)
    val mapped = state.mapper(struct)

    writerManager
      .writer(dir, tpo.toTopicPartition, mapped.schema)
      .write(mapped)

    state = state.withTopicPartitionOffset(tpo)
  }

  def flush(): Unit = {
    Option(state).map(_.offsets).foreach(writerManager.flush)
  }

  def commit(): Map[TopicPartition, Offset] = {
    Option(state).map { _ =>
      val newCommittedOffsets = {
        for {
          writer <- writerManager.getWriters
          offset <- state.offsets.get(writer.tp)
          tpo = TopicPartitionOffset(writer.tp.topic, writer.tp.partition, offset)
          cc = CommitContext(tpo, writer.dir, writer.writer.currentCount, writer.writer.fileSize, writer.writer.createdTime)
          if tableConfig.commitPolicy.shouldFlush(cc)
        } yield {
          logger.info(s"Flushing offsets for ${writer.dir} on topicpart ${writer.tp}")
          writerManager.flush(tpo, writer.dir)
          writer.tp -> offset
        }
        }.toMap

      state = state.withCommittedOffset(newCommittedOffsets)
      newCommittedOffsets
    }.getOrElse(Map.empty)
  }

  def close(): Unit = flush()

  def getState: Option[HiveSinkState] = Option(state)
}

object HiveSink {
  def from(tableName: TableName,
           config: HiveSinkConfig)
          (implicit client: IMetaStoreClient, fs: FileSystem): HiveSink = {
    val tableConfig = config.tableOptions
      .find(_.tableName == tableName)
      .getOrElse(throw new ConnectException(s"No table config for ${tableName.value}"))

    new HiveSink(tableConfig, config.stageManager, config.dbName)
  }
}