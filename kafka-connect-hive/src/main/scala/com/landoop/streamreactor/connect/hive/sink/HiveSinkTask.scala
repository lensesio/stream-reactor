package com.landoop.streamreactor.connect.hive.sink

import java.io.IOException
import java.util

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.sink.config.HDFSSinkConfigConstants
import com.landoop.streamreactor.connect.hive.sink.config.HiveSinkConfig
import com.landoop.streamreactor.connect.hive.sink.staging.OffsetSeeker
import com.landoop.streamreactor.connect.hive.HDFSConfigurationExtension._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import scala.collection.JavaConverters._
import scala.concurrent.duration._
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
  private var kerberosTicketRenewal: AsyncFunctionLoop = _

  def this(fs: FileSystem, client: HiveMetaStoreClient) {
    this()
    this.client = client
    this.fs = fs
  }

  override def version(): String = manifest.version()

  override def start(props: util.Map[String, String]): Unit = {

    val configs = if (context.configs().isEmpty) props else context.configs()

    config = HiveSinkConfig.fromProps(configs.asScala.toMap)

    if (client == null) {
      val hiveConf = ConfigurationBuilder.buildHiveConfig(config.hadoopConfiguration)
      hiveConf.set("hive.metastore", configs.get(HDFSSinkConfigConstants.MetastoreTypeKey))
      hiveConf.set("hive.metastore.uris", configs.get(HDFSSinkConfigConstants.MetastoreUrisKey))
      client = new HiveMetaStoreClient(hiveConf)
    }

    if (fs == null) {
      val conf: Configuration = ConfigurationBuilder.buildHdfsConfiguration(config.hadoopConfiguration)
      conf.set("fs.defaultFS", configs.get(HDFSSinkConfigConstants.FsDefaultKey))

      config.kerberos.foreach { kerberos =>
        val ugi = conf.getUGI(kerberos)
        conf.withKerberos(kerberos)
        val interval = kerberos.ticketRenewalMs.millis
        kerberosTicketRenewal = new AsyncFunctionLoop(interval, "Kerberos")(renewKerberosTicket(ugi))
      }

      fs = FileSystem.get(conf)
    }
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

  private def renewKerberosTicket(ugi: UserGroupInformation): Unit = {
    try {
      ugi.reloginFromKeytab()
    }
    catch {
      case e: IOException =>
        // We ignore this exception during relogin as each successful relogin gives
        // additional 24 hours of authentication in the default config. In normal
        // situations, the probability of failing relogin 24 times is low and if
        // that happens, the task will fail eventually.
        logger.error("Error renewing the Kerberos ticket", e)
    }
  }
}