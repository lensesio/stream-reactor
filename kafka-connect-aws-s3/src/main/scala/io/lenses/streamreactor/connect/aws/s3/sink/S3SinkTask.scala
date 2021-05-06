
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

import com.datamountaineer.streamreactor.common.errors.RetryErrorPolicy
import com.datamountaineer.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.aws.s3.auth.AwsContextCreator
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.sink.conversion.HeaderToStringConverter
import io.lenses.streamreactor.connect.aws.s3.sink.conversion.ValueToSinkDataConverter
import io.lenses.streamreactor.connect.aws.s3.storage.MultipartBlobStoreStorageInterface
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
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

  private var storageInterface: StorageInterface = _

  private var config: S3SinkConfig = _

  private var sinkName: String = _

  override def version(): String = manifest.version()

  def validateBuckets(storageInterface: StorageInterface, config: S3SinkConfig) = {
    config.bucketOptions.foreach(
      bucketOption => {
        val bucketAndPrefix = bucketOption.bucketAndPrefix
        storageInterface.list(bucketAndPrefix)
      }
    )
  }

  override def start(props: util.Map[String, String]): Unit = {
    sinkName = Option(props.get("name")).getOrElse("MissingSinkName")

    logger.debug(s"[{}] S3SinkTask.start", sinkName)

    val awsConfig = S3SinkConfig(props.asScala.toMap)

    val awsContextCreator = new AwsContextCreator(AwsContextCreator.DefaultCredentialsFn)
    storageInterface = new MultipartBlobStoreStorageInterface(sinkName, awsContextCreator.fromConfig(awsConfig.s3Config))

    val configs = Option(context).flatMap(c => Option(c.configs())).filter(_.isEmpty == false).getOrElse(props)

    config = S3SinkConfig(configs.asScala.toMap)

    validateBuckets(storageInterface, config)

    setErrorRetryInterval

    writerManager = S3WriterManager.from(config, sinkName)(storageInterface)
  }

  private def setErrorRetryInterval = {
    //if error policy is retry set retry interval
    config.s3Config.errorPolicy match {
      case RetryErrorPolicy() => context.timeout(config.s3Config.connectorRetryConfig.errorRetryInterval)
      case _ =>
    }
  }

  case class TopicAndPartition(topic: String, partition: Int) {
    override def toString(): String = s"$topic-$partition"
  }

  object TopicAndPartition {
    implicit val ordering: Ordering[TopicAndPartition] = (x: TopicAndPartition, y: TopicAndPartition) => {
      val c = x.topic.compareTo(y.topic)
      if (c == 0) x.partition.compareTo(y.partition)
      else c
    }
  }

  case class Bounds(start: Long, end: Long) {
    override def toString: String = s"$start->$end"
  }

  def buildLogForRecords(records: Iterable[SinkRecord]): Map[TopicAndPartition, Bounds] = {
    records.foldLeft(Map.empty[TopicAndPartition, Bounds]) { case (map, record) =>
      val topicAndPartition = TopicAndPartition(record.topic(), record.kafkaPartition())
      map.get(topicAndPartition) match {
        case Some(value) =>
          map + (topicAndPartition -> value.copy(end = record.kafkaOffset()))
        case None => map + (topicAndPartition -> Bounds(record.kafkaOffset(), record.kafkaOffset()))
      }
    }
  }
  override def put(records: util.Collection[SinkRecord]): Unit = {
    val recordsStats = buildLogForRecords(records.asScala)
      .toList.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString(";")

    logger.debug(s"[$sinkName] put records=${records.size()} stats=$recordsStats")

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
    def getDebugInfo(in: util.Map[KafkaTopicPartition, OffsetAndMetadata]): String = {
      in.entrySet().asScala.toList.map(e => e.getKey.topic() + "-" + e.getKey.partition() + ":" + e.getValue.offset()).mkString(";")
    }
    logger.debug(s"[{}] preCommit with offsets={}", sinkName, getDebugInfo(currentOffsets): Any)

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

    val actualOffsets = writerManager
      .preCommit(topicPartitionOffsetTransformed)
      .map {
        case (topicPartition, offsetAndMetadata) =>
          (topicPartition.toKafka, offsetAndMetadata)
      }.asJava

    logger.debug(s"[{}] Returning latest written offsets={}", sinkName: Any, getDebugInfo(actualOffsets): Any)
    actualOffsets
  }

  override def open(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    val partitionsDebug = partitions.asScala.map(tp => s"${tp.topic()}-${tp.partition()}").mkString(",")
    logger.debug(s"[{}] Open partitions", sinkName: Any, partitionsDebug: Any)

    try {
      val topicPartitions = partitions.asScala
        .map(tp => TopicPartition(Topic(tp.topic), tp.partition))
        .toSet

      writerManager.open(topicPartitions)
        .foreach {
          case (topicPartition, offset) =>
            logger.debug(s"[$sinkName] Seeking to ${topicPartition.topic.value}-${topicPartition.partition}:${offset.value}")
            context.offset(topicPartition.toKafka, offset.value)
        }

    } catch {
      case NonFatal(e) =>
        logger.error(s"[$sinkName] Error opening s3 sink writer", e)
        throw e
    }
  }

  /**
    * Whenever close is called, the topics and partitions assigned to this task
    * may be changing, eg, in a re-balance. Therefore, we must commit our open files
    * for those (topic,partitions) to ensure no records are lost.
    */
  override def close(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    logger.debug(s"[{}] S3SinkTask.close with {} partitions", sinkName, partitions.size())

    writerManager.close()
  }

  override def stop(): Unit = {
    logger.debug(s"[{}] Stop")

    writerManager.close()
    writerManager = null
  }

}
