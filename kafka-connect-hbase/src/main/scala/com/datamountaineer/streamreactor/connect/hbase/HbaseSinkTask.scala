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
package io.lenses.streamreactor.connect.hbase

import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.common.utils.ProgressCounter
import io.lenses.streamreactor.connect.hbase.config.ConfigurationBuilder
import io.lenses.streamreactor.connect.hbase.config.HBaseConfig
import io.lenses.streamreactor.connect.hbase.config.HBaseConfigConstants
import io.lenses.streamreactor.connect.hbase.config.HBaseSettings
import io.lenses.streamreactor.connect.hbase.config.HBaseConfigExtension._
import io.lenses.streamreactor.connect.hbase.kerberos.KerberosLogin
import io.lenses.streamreactor.connect.hbase.writers.HbaseWriter
import io.lenses.streamreactor.connect.hbase.writers.WriterFactoryFn
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
  * <h1>HbaseSinkTask</h1>
  *
  * Kafka Connect Cassandra sink task. Called by framework to put records to the
  * target sink
  */
class HbaseSinkTask extends SinkTask with StrictLogging {

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  var writer: Option[HbaseWriter] = None
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false

  /**
    * Parse the configurations and setup the writer
    */
  override def start(props: util.Map[String, String]): Unit = {

    printAsciiHeader(manifest, "/hbase-ascii.txt")

    val conf = if (context.configs().isEmpty) props else context.configs()

    HBaseConfig.config.parse(conf)
    val sinkConfig = HBaseConfig(conf)
    enableProgress = sinkConfig.getBoolean(HBaseConfigConstants.PROGRESS_COUNTER_ENABLED)
    val hbaseSettings = HBaseSettings(sinkConfig)

    //if error policy is retry set retry interval
    hbaseSettings.errorPolicy match {
      case RetryErrorPolicy() => context.timeout(sinkConfig.getInt(HBaseConfigConstants.ERROR_RETRY_INTERVAL).toLong)
      case _                  =>
    }

    val hbaseConf = ConfigurationBuilder.buildHBaseConfig(hbaseSettings)

    hbaseSettings.kerberos.map { kerberos =>
      hbaseConf.withKerberos(kerberos)
      KerberosLogin.from(kerberos, hbaseConf)
    }

    logger.info(
      s"""Settings:
         |$hbaseSettings
      """.stripMargin,
    )

    writer = Some(WriterFactoryFn(hbaseSettings, hbaseConf))

  }

  /**
    * Pass the SinkRecords to the writer for Writing
    */
  override def put(records: util.Collection[SinkRecord]): Unit =
    if (records.size() == 0) {
      logger.info("Empty list of records received.")
    } else {
      require(writer.nonEmpty, "Writer is not set!")
      val seq = records.asScala.toVector
      writer.foreach(w => w.write(seq))

      if (enableProgress) {
        progressCounter.update(seq)
      }
    }

  /**
    * Clean up Hbase connections
    */
  override def stop(): Unit = {
    logger.info("Stopping Hbase sink.")
    writer.foreach(w => w.close())
    progressCounter.empty()
  }

  override def version(): String = manifest.version()

  override def flush(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}
}
