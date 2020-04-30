package com.landoop.streamreactor.connect.hive.sink

import java.util

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.landoop.streamreactor.connect.hive.HadoopConfigurationExtension._
import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.kerberos.KerberosLogin
import com.landoop.streamreactor.connect.hive.sink.config.{HiveSinkConfig, SinkConfigSettings}
import com.landoop.streamreactor.connect.hive.sink.staging.OffsetSeeker
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

class HiveSinkTask extends SinkTask {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  // this map contains all the open sinks for this task
  // They are created when the topic/partition assignment happens (the 'open' method)
  // and they are removed when unassignment happens (the 'close' method)
  private val sinks = scala.collection.mutable.Map.empty[TopicPartition, HiveSink]

  private var client: HiveMetaStoreClient = _
  private var fs: FileSystem = _
  private var config: HiveSinkConfig = _
  private var kerberosLogin = Option.empty[KerberosLogin]

  def this(fs: FileSystem, client: HiveMetaStoreClient) {
    this()
    this.client = client
    this.fs = fs
  }

  override def version(): String = manifest.version()

  override def start(props: util.Map[String, String]): Unit = {

    val configs = Option(context).flatMap(c => Option(c.configs())).filter(_.isEmpty == false).getOrElse(props)

    config = HiveSinkConfig.fromProps(configs.asScala.toMap)

    Option(fs).foreach(fs => Try(fs.close()))
    Option(client).foreach(c => Try(c.close()))

    val conf: Configuration = ConfigurationBuilder.buildHdfsConfiguration(config.hadoopConfiguration)
    conf.set("fs.defaultFS", configs.get(SinkConfigSettings.FsDefaultKey))

    kerberosLogin = config.kerberos.map { kerberos =>
      conf.withKerberos(kerberos)
      KerberosLogin.from(kerberos, conf)
    }

    val hiveConf = ConfigurationBuilder.buildHiveConfig(config.hadoopConfiguration)
    hiveConf.set("hive.metastore.local", "false")
    hiveConf.set("hive.metastore", configs.get(SinkConfigSettings.MetastoreTypeKey))
    hiveConf.set("hive.metastore.uris", configs.get(SinkConfigSettings.MetastoreUrisKey))
    config.kerberos.foreach { _ =>
      val principal = Option(configs.get(SinkConfigSettings.HiveMetastorePrincipalKey))
        .getOrElse {
          throw new ConnectException(s"Missing configuration for [${SinkConfigSettings.HiveMetastorePrincipalKey}]. When using Kerberos it is required to set the configuration.")
        }

      hiveConf.set("hive.metastore.kerberos.principal", principal)
    }

    def initialize(): Unit = {
      fs = FileSystem.get(conf)
      client = new HiveMetaStoreClient(hiveConf)
    }
    kerberosLogin.fold(initialize())(_.run(initialize()))

    val databases = execute(client.getAllDatabases)
    if (!databases.contains(config.dbName.value)) {
      throw new ConnectException(s"Cannot find database [${config.dbName.value}]. Current database(-s): ${databases.asScala.mkString(",")}. Please make sure the database is created before sinking to it.")
    }
  }

  override def put(records: util.Collection[SinkRecord]): Unit = execute {
    records.asScala.foreach { record =>
      val struct = ValueConverter(record)
      val tpo = TopicPartitionOffset(
        Topic(record.topic),
        record.kafkaPartition.intValue,
        Offset(record.kafkaOffset)
      )
      val tp = tpo.toTopicPartition
      sinks
        .getOrElse(tp, throw new ConnectException(s"Could not find $tp in sinks $sinks"))
        .write(struct, tpo)
    }

    sinks.values.foreach(_.commit())
  }

  override def preCommit(currentOffsets: util.Map[KafkaTopicPartition, OffsetAndMetadata]): util.Map[KafkaTopicPartition, OffsetAndMetadata] = {
    val committedOffsets = sinks.values
      .flatMap(_.getState)
      .flatMap(_.committedOffsets)
      .flatMap { case (k, v) =>
        val topicPartition = new KafkaTopicPartition(k.topic.value, k.partition)
        Option(currentOffsets.get(topicPartition))
          .map { offset =>
            topicPartition -> new OffsetAndMetadata(v.value, offset.leaderEpoch(), offset.metadata())
          }
      }.toMap

    committedOffsets.asJava
  }

  override def open(partitions: util.Collection[KafkaTopicPartition]): Unit = execute {
    try {
      val topicPartitions = partitions.asScala
        .map(tp => TopicPartition(Topic(tp.topic), tp.partition))
        .toSet

      open(topicPartitions)
    } catch {
      case NonFatal(e) =>
        logger.error("Error opening hive sink writer", e)
        throw e
    }
  }

  private def open(partitions: Set[TopicPartition]): Unit = {
    val seeker = new OffsetSeeker(config.filenamePolicy)
    // we group by table name, so we only need to seek once per table
    partitions.groupBy(tp => table(tp.topic)).foreach { case (tableName, tps) =>
      val offsets = seeker.seek(config.dbName, tableName)(fs, client)
      // can be multiple topic/partitions writing to the same table
      tps.foreach { tp =>
        offsets
          .find(_.toTopicPartition == tp)
          .foreach { case TopicPartitionOffset(topic, partition, offset) =>
            logger.info(s"Seeking to ${topic.value}:$partition:${offset.value}")
            context.offset(new KafkaTopicPartition(topic.value, partition), offset.value)
          }
        logger.info(s"Opening sink for ${config.dbName.value}.${tableName.value} for ${tp.topic.value}:${tp.partition}")
        val sink: HiveSink = HiveSink.from(tableName, config)(client, fs)
        sinks.put(tp, sink)
      }
    }
  }

  /**
   * Whenever close is called, the topics and partitions assigned to this task
   * may be changing, eg, in a rebalance. Therefore, we must commit our open files
   * for those (topic,partitions) to ensure no records are lost.
   */
  override def close(partitions: util.Collection[KafkaTopicPartition]): Unit = execute {
    val topicPartitions = partitions.asScala.map(tp => TopicPartition(Topic(tp.topic), tp.partition)).toSet
    close(topicPartitions)
  }

  // closes the sinks for the given topic/partition tuples
  private def close(partitions: Set[TopicPartition]): Unit = {
    partitions.foreach { tp =>
      sinks.remove(tp).foreach(_.close())
    }
  }

  override def stop(): Unit = {
    execute(sinks.values.foreach(_.close()))
    sinks.clear()
    kerberosLogin.foreach(_.close())
    kerberosLogin = None
  }

  // returns the KCQL table name for the given topic
  private def table(topic: Topic): TableName = config.tableOptions.find(_.topic == topic) match {
    case Some(options) => options.tableName
    case _ => sys.error(s"Cannot find KCQL for topic $topic")
  }

  private def execute[T](thunk: => T): T = {
    kerberosLogin match {
      case None => thunk
      case Some(login) => login.run(thunk)
    }
  }
}