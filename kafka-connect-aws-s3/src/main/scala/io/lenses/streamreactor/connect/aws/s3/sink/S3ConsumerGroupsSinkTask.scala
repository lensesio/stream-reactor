/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.sink

import cats.implicits.toShow
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.CONNECTOR_PREFIX
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3ConsumerGroupsSinkConfig
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3Uploader
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskIdCreator
import io.lenses.streamreactor.connect.cloud.common.consumers.ConsumerGroupsWriter
import io.lenses.streamreactor.connect.cloud.common.utils.MapUtils
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

/**
  * A Kafka connector designed to persist the most recent Kafka consumer group offsets from the `__consumer_offsets` topic to an S3 storage.
  * The connector adheres to an eventually consistent model. It captures and stores the latest offset for each partition within each consumer group.
  * These offsets are organized in S3 objects using the following key structure: `bucket/prefix.../consumerGroup/topic/partition`, with the offset value represented as a long.
  *
  * However, it's important to note that the connector does not actively track consumer groups that drop topic subscriptions or unsubscribe from topics. As a result, it does not automatically remove redundant keys.
  * Also the writes are eventually consistent. Depending on Connect replaying messages, the offsets may be written multiple times.
  * But since the s3 key is unique for group-topic-partition the last write will be the latest offset.
  */

class S3ConsumerGroupsSinkTask extends SinkTask with ErrorHandler {

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  private var connectorTaskId: ConnectorTaskId      = _
  private var writerManager:   ConsumerGroupsWriter = _

  override def version(): String = manifest.version()

  override def start(fallbackProps: util.Map[String, String]): Unit = {

    printAsciiHeader(manifest, "/aws-s3-cg-sink-ascii.txt")

    logger.debug(s"[{}] S3ConsumerGroupSinkTask.start", fallbackProps.get("name"))

    val contextProps = Option(context).flatMap(c => Option(c.configs())).map(_.asScala.toMap).getOrElse(Map.empty)
    val props        = MapUtils.mergeProps(contextProps, fallbackProps.asScala.toMap).asJava
    (for {
      taskId   <- new ConnectorTaskIdCreator(CONNECTOR_PREFIX).fromProps(fallbackProps)
      config   <- S3ConsumerGroupsSinkConfig.fromProps(props)
      s3Client <- AwsS3ClientCreator.make(config.config)
      uploader  = new AwsS3Uploader(s3Client, taskId)
    } yield new ConsumerGroupsWriter(config.location, uploader, taskId) -> taskId) match {
      case Left(value) => throw value
      case Right((writer, taskId)) =>
        writerManager   = writer
        connectorTaskId = taskId
    }
  }

  override def put(records: util.Collection[SinkRecord]): Unit =
    writerManager.write(records.asScala.toList) match {
      case Left(ex) =>
        logger.error(s"[{}] Failed to write records to S3",
                     Option(connectorTaskId).map(_.show).getOrElse("Unnamed"),
                     ex,
        )
        throw ex
      case Right(_) => ()
    }

  override def close(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    logger.debug(
      "[{}] S3ConsumerGroupsSinkTask.close with {} partitions",
      Option(connectorTaskId).map(_.show).getOrElse("Unnamed"),
      partitions.size(),
    )

    Option(writerManager).foreach(_.close())
  }

  override def stop(): Unit = {
    logger.debug("[{}] Stop", Option(connectorTaskId).map(_.show).getOrElse("Unnamed"))

    Option(writerManager).foreach(_.close())
    writerManager = null
  }

}
