package io.lenses.streamreactor.connect.aws.s3.sink

import java.util

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import io.lenses.streamreactor.connect.aws.s3._
import io.lenses.streamreactor.connect.aws.s3.auth.AwsContextCreator
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import io.lenses.streamreactor.connect.aws.s3.storage.{MultipartBlobStoreStorageInterface, StorageInterface}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class S3SinkTask extends SinkTask {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  private var writerManager: S3WriterManager = _

  private var storageInterface: StorageInterface = _

  private var config: S3Config = _

  override def version(): String = manifest.version()

  override def start(props: util.Map[String, String]): Unit = {

    logger.debug(s"Received call to S3SinkTask.start with ${props.size()} properties")

    val awsConfig = S3Config(props.asScala.toMap)


    storageInterface = new MultipartBlobStoreStorageInterface(AwsContextCreator.fromConfig(awsConfig))

    val configs = Option(context).flatMap(c => Option(c.configs())).filter(_.isEmpty == false).getOrElse(props)

    config = S3Config(configs.asScala.toMap)

    writerManager = S3WriterManager.from(config)(storageInterface)

  }

  override def put(records: util.Collection[SinkRecord]): Unit = {

    logger.debug(s"Received call to S3SinkTask.put with ${records.size()} records")

    records.asScala.foreach {
      record =>
        val struct = ValueConverter(record)
        val tpo = TopicPartitionOffset(
          Topic(record.topic),
          record.kafkaPartition.intValue,
          Offset(record.kafkaOffset)
        )
        writerManager.write(tpo, struct)
    }

    if (records.isEmpty) writerManager.commit()
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

      writerManager.open(topicPartitions)
        .foreach {
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
    * may be changing, eg, in a rebalance. Therefore, we must commit our open files
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