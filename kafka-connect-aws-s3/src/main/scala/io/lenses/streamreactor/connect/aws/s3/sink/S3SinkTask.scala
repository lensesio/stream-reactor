
/*
 * Copyright 2020 Lenses.io
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

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import io.lenses.streamreactor.connect.aws.s3.auth.AwsContextCreator
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.commit.Committer
import io.lenses.streamreactor.connect.aws.s3.sink.commit.WatermarkSeeker
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.sink.conversion.HeaderToStringConverter
import io.lenses.streamreactor.connect.aws.s3.sink.conversion.ValueToSinkDataConverter
import io.lenses.streamreactor.connect.aws.s3.storage.MultipartBlobStoreStorage
import io.lenses.streamreactor.connect.aws.s3.storage.Storage
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class S3SinkTask extends SinkTask {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  private var writerManager: S3WriterManager = _

  private var storage: Storage = _

  private var config: S3SinkConfig = _

  private var committer: Committer = _

  override def version(): String = manifest.version()

  def validateBuckets(storageInterface: Storage, config: S3SinkConfig): Unit = {
    config.bucketOptions.foreach { bucketOption =>
      val bucketAndPrefix = bucketOption.bucketAndPrefix
      storageInterface.pathExists(bucketAndPrefix)
    }
  }

  override def start(props: util.Map[String, String]): Unit = {

    logger.debug(s"Received call to S3SinkTask.start with ${props.size()} properties")

    val awsConfig = S3SinkConfig(props.asScala.toMap)

    val awsContextCreator = new AwsContextCreator(AwsContextCreator.DefaultCredentialsFn)
    storage = new MultipartBlobStoreStorage(awsContextCreator.fromConfig(awsConfig.s3Config))

    val configs = Option(context).flatMap(c => Option(c.configs())).filter(_.isEmpty == false).getOrElse(props)

    config = S3SinkConfig(configs.asScala.toMap)

    validateBuckets(storage, config)

    committer = Committer.from(config, storage)
    writerManager = S3WriterManager.from(config, storage, committer)
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {

    logger.debug(s"Received call to S3SinkTask.put with ${records.size()} records")

    records.asScala.foreach {
      record =>
        val tpo = TopicPartitionOffset(
          Topic(record.topic),
          record.kafkaPartition.intValue,
          Offset(record.kafkaOffset)
        )
        writerManager.write(
          tpo,
          MessageDetail(
            keySinkData = Option(record.key()).fold(Option.empty[SinkData])(key => Option(ValueToSinkDataConverter(key, Option(record.keySchema())))),
            valueSinkData = ValueToSinkDataConverter(record.value(), Option(record.valueSchema())),
            headers = HeaderToStringConverter(record)
          )
        )
    }

    if (records.isEmpty) writerManager.commitAllWritersIfFlushRequired()
  }

  override def preCommit(currentOffsets: util.Map[KafkaTopicPartition, OffsetAndMetadata]): util.Map[KafkaTopicPartition, OffsetAndMetadata] = {

    logger.debug(s"Received call to S3SinkTask.preCommit with current offsets ${currentOffsets.values()}")

    val topicPartitionOffsetTransformed: Map[TopicPartition, OffsetAndMetadata] = currentOffsets
      .asScala
      .map {
        topicPartToOffsetTuple: (KafkaTopicPartition, OffsetAndMetadata) =>
          (
            TopicPartition(topicPartToOffsetTuple._1),
            topicPartToOffsetTuple._2
          )
      }
      .toMap

    writerManager
      .preCommit(topicPartitionOffsetTransformed)
      .map {
        case (topicPartition, offsetAndMetadata) =>
          (topicPartition.toKafka, offsetAndMetadata)
      }
      .asJava

  }

  override def open(partitions: util.Collection[KafkaTopicPartition]): Unit = {

    logger.debug(s"Received call to S3SinkTask.open with ${partitions.size()} partitions")

    try {
      val topicPartitions = partitions.asScala
        .map(tp => TopicPartition(Topic(tp.topic), tp.partition))
        .toSet

      committer.latest(topicPartitions).foreach {
        case (topicPartition, offset) =>
          logger.debug(s"Seeking to ${topicPartition.topic.value}:${topicPartition.partition}:${offset.value}")
          context.offset(topicPartition.toKafka, offset.value)
      }
    } catch {
      case NonFatal(e) =>
        logger.error("Error opening s3 sink writer", e)
        throw e
    }
  }

  /**
    * Whenever close is called, the topics and partitions assigned to this task
    * may be changing, eg, in a re-balance. Therefore, we must commit our open files
    * for those (topic,partitions) to ensure no records are lost.
    */
  override def close(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    logger.debug(s"Received call to S3SinkTask.close with ${partitions.size()} partitions")

    writerManager.close()
  }

  override def stop(): Unit = {
    logger.debug(s"Received call to S3SinkTask.stop")

    writerManager.close()
    writerManager = null
  }

}
