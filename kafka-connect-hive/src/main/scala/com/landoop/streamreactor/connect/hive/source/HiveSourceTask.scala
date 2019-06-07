package com.landoop.streamreactor.connect.hive.source

import java.util

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.landoop.streamreactor.connect.hive.sink.config.HDFSSinkConfigConstants
import com.landoop.streamreactor.connect.hive.source.config.HiveSourceConfig
import com.landoop.streamreactor.connect.hive.source.offset.HiveSourceOffsetStorageReader
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask

import scala.collection.JavaConverters._

class HiveSourceTask extends SourceTask with StrictLogging {

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)
  private var client: HiveMetaStoreClient = _
  private var fs: FileSystem = _
  private var config: HiveSourceConfig = _

  private var sources: Set[HiveSource] = Set.empty
  private var iterator: Iterator[SourceRecord] = Iterator.empty

  def this(fs: FileSystem, client: HiveMetaStoreClient) {
    this()
    this.client = client
    this.fs = fs
  }

  override def start(props: util.Map[String, String]): Unit = {
    val configs = if (context.configs().isEmpty) props else context.configs()

    if (client == null) {
      val hiveConf = new HiveConf()
      hiveConf.set("hive.metastore", configs.get(HDFSSinkConfigConstants.MetastoreTypeKey))
      hiveConf.set("hive.metastore.uris", configs.get(HDFSSinkConfigConstants.MetastoreUrisKey))
      client = new HiveMetaStoreClient(hiveConf)
    }

    if (fs == null) {
      val conf = new Configuration()
      conf.set("fs.defaultFS", configs.get(HDFSSinkConfigConstants.FsDefaultKey))
      fs = FileSystem.get(conf)
    }

    config = HiveSourceConfig.fromProps(configs.asScala.toMap)

    sources = config.tableOptions.map { options =>
      new HiveSource(
        config.dbName,
        options.tableName,
        options.topic,
        new HiveSourceOffsetStorageReader(context.offsetStorageReader),
        config
      )(client, fs)
    }

    iterator = sources.reduce((a: Iterator[SourceRecord], b: Iterator[SourceRecord]) => a ++ b)
  }

  override def poll(): util.List[SourceRecord] = {
    iterator.take(config.pollSize).toList.asJava
  }

  override def stop(): Unit = sources.foreach(_.close)

  override def version(): String = manifest.version()
}