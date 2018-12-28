package com.landoop.streamreactor.connect.hive.sink

import java.util

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.sink.config.{HiveSinkConfig, HiveSinkConfigConstants}
import com.landoop.streamreactor.connect.hive.sink.staging.OffsetSeeker
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class HiveSinkTask extends SinkTask with StrictLogging {

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  // this map contains all the open sinks for this task
  // They are created when the topic/partition assignment happens (the 'open' method)
  // and they are removed when unassignment happens (the 'close' method)
  private val sinks = scala.collection.mutable.Map.empty[TopicPartition, HiveSink]

  private var client: HiveMetaStoreClient = _
  private var fs: FileSystem = _
  private var config: HiveSinkConfig = _

  def this(fs: FileSystem, client: HiveMetaStoreClient) {
    this()
    this.client = client
    this.fs = fs
  }

  override def version(): String = manifest.version()

  override def start(props: util.Map[String, String]): Unit = {

    if (client == null) {
      val hiveConf = new HiveConf()
      hiveConf.set("hive.metastore", props.get(HiveSinkConfigConstants.MetastoreTypeKey))
      hiveConf.set("hive.metastore.uris", props.get(HiveSinkConfigConstants.MetastoreUrisKey))
      client = new HiveMetaStoreClient(hiveConf)
    }

    if (fs == null) {
      val conf: Configuration = new Configuration()
      conf.set("fs.defaultFS", props.get(HiveSinkConfigConstants.FsDefaultKey))
      fs = FileSystem.get(conf)
    }

    config = HiveSinkConfig.fromProps(props.asScala.toMap)
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    records.asScala.foreach { record =>
      val struct = ValueConverter(record)
      val tpo = TopicPartitionOffset(
        Topic(record.topic),
        record.kafkaPartition.intValue,
        Offset(record.kafkaOffset)
      )
      val tp = tpo.toTopicPartition
      sinks.getOrElse(tp, sys.error(s"Could not find $tp in sinks $sinks")).write(struct, tpo)
    }
  }

  override def open(partitions: util.Collection[KafkaTopicPartition]): Unit = try {
    val topicPartitions = partitions.asScala.map(tp => TopicPartition(Topic(tp.topic), tp.partition)).toSet
    open(topicPartitions)
  } catch {
    case NonFatal(e) =>
      logger.error("Error opening hive sink writer", e)
      throw e
  }

  private def open(partitions: Set[TopicPartition]): Unit = {
    val seeker = new OffsetSeeker(config.filenamePolicy)
    // we group by table name, so we only need to seek once per table
    partitions.groupBy(tp => table(tp.topic)).foreach { case (tableName, tps) =>
      val offsets = seeker.seek(config.dbName, tableName)(fs, client)
      // can be multiple topic/partitions writing to the same table
      tps.foreach { tp =>
        offsets.find(_.toTopicPartition == tp).foreach { case TopicPartitionOffset(topic, partition, offset) =>
          logger.info(s"Seeking to ${topic.value}:$partition:${offset.value}")
          context.offset(new KafkaTopicPartition(topic.value, partition), offset.value)
        }
        logger.info(s"Opening sink for ${config.dbName.value}.${tableName.value} for ${tp.topic.value}:${tp.partition}")
        val sink = new HiveSink(tableName, config)(client, fs)
        sinks.put(tp, sink)
      }
    }
  }

  /**
    * Whenever close is called, the topics and partitions assigned to this task
    * may be changing, eg, in a rebalance. Therefore, we must commit our open files
    * for those (topic,partitions) to ensure no records are lost.
    */
  override def close(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    val topicPartitions = partitions.asScala.map(tp => TopicPartition(Topic(tp.topic), tp.partition)).toSet
    close(topicPartitions)
  }

  // closes the sinks for the given topic/partition tuples
  def close(partitions: Set[TopicPartition]): Unit = {
    partitions.foreach { tp =>
      sinks.remove(tp).foreach(_.close)
    }
  }

  override def stop(): Unit = {
    sinks.values.foreach(_.close)
    sinks.clear()
  }

  // returns the KCQL table name for the given topic
  private def table(topic: Topic): TableName = config.tableOptions.find(_.topic == topic) match {
    case Some(options) => options.tableName
    case _ => sys.error(s"Cannot find KCQL for topic $topic")
  }
}