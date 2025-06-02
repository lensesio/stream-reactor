/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink

import cats.implicits.toBifunctorOps
import cats.implicits.toShow
import io.lenses.streamreactor.common.config.base.intf.ConnectionConfig
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.util.JarManifest
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskIdCreator
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.HeaderToSinkDataConverter
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.ValueToSinkDataConverter
import io.lenses.streamreactor.connect.cloud.common.sink.optimization.AttachLatestSchemaOptimizer
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.utils.MapUtils
import io.lenses.streamreactor.connect.cloud.common.utils.TimestampUtils
import io.lenses.streamreactor.metrics.Metrics
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Try

/**
 * @param connectorPrefix
 * @param sinkAsciiArtResource
 * @param manifest
 * @tparam MD file metadata type
 * @tparam C cloud sink config subtype
 * @tparam CC connection configuration type
 * @tparam CT client type
 */
abstract class CloudSinkTask[MD <: FileMetadata, C <: CloudSinkConfig[CC], CC <: ConnectionConfig, CT](
  connectorPrefix:      String,
  sinkAsciiArtResource: String,
  manifest:             JarManifest,
) extends SinkTask
    with ErrorHandler {

  private val writerManagerCreator = new WriterManagerCreator[MD, C]()

  private var logMetrics = false
  private var writerManager: WriterManager[MD] = _
  private var indexManager:  IndexManager      = _
  private var config:        C                 = _
  private val attachLatestSchemaOptimizer = new AttachLatestSchemaOptimizer()
  implicit var connectorTaskId: ConnectorTaskId = _

  override def version(): String = manifest.getVersion()

  override def start(fallbackProps: util.Map[String, String]): Unit = {

    printAsciiHeader(manifest, sinkAsciiArtResource)

    new ConnectorTaskIdCreator(connectorPrefix).fromProps(fallbackProps.asScala.toMap) match {
      case Left(value)  => throw new IllegalArgumentException(value)
      case Right(value) => connectorTaskId = value
    }

    logger.debug(s"[{}] CloudSinkTask.start", connectorTaskId.show)

    val contextProps   = Option(context).flatMap(c => Option(c.configs())).map(_.asScala.toMap).getOrElse(Map.empty)
    val props          = MapUtils.mergeProps(contextProps, fallbackProps.asScala.toMap)
    val errOrWriterMan = createWriterMan(props)

    errOrWriterMan.leftMap(throw _).foreach {
      case (im, wm, c) =>
        indexManager  = im
        writerManager = wm
        config        = c
    }
  }

  private def rollback(topicPartitions: Set[TopicPartition]): Unit =
    topicPartitions.foreach(writerManager.cleanUp)

  private def handleErrors(value: Either[SinkError, Unit]): Unit =
    value match {
      case Left(error: SinkError) =>
        if (error.rollBack()) {
          rollback(error.topicPartitions())
        }
        throw new ConnectException(error.message(), error.exception().orNull)
      case Right(_) =>
    }

  case class TopicAndPartition(topic: String, partition: Int) {
    override def toString: String = s"$topic-$partition"
  }

  private object TopicAndPartition {
    implicit val ordering: Ordering[TopicAndPartition] = (x: TopicAndPartition, y: TopicAndPartition) => {
      val c = x.topic.compareTo(y.topic)
      if (c == 0) x.partition.compareTo(y.partition)
      else c
    }
  }

  private case class Bounds(start: Long, end: Long) {
    override def toString: String = s"$start->$end"
  }

  private def buildLogForRecords(records: Iterable[SinkRecord]): Map[TopicAndPartition, Bounds] =
    records.foldLeft(Map.empty[TopicAndPartition, Bounds]) {
      case (map, record) =>
        val topicAndPartition = TopicAndPartition(record.topic(), record.kafkaPartition())
        map.get(topicAndPartition) match {
          case Some(value) =>
            map + (topicAndPartition -> value.copy(end = record.kafkaOffset()))
          case None => map + (topicAndPartition -> Bounds(record.kafkaOffset(), record.kafkaOffset()))
        }
    }

  override def put(records: util.Collection[SinkRecord]): Unit =
    Metrics.withTimer {
      handleTry {
        Try {

          logger.debug(
            s"[${connectorTaskId.show}] put records=${records.size()} stats=${buildLogForRecords(records.asScala)
              .toList.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString(";")}",
          )

          // a failure in recommitPending will prevent the processing of further records
          handleErrors(writerManager.recommitPending())

          //check if the optimizer on schema is enabled
          val updatedRecords: Iterable[SinkRecord] =
            if (config.latestSchemaForWriteEnabled) {
              attachLatestSchemaOptimizer.update(records.asScala.toList)
            } else {
              records.asScala
            }

          updatedRecords.foreach {
            record =>
              val topicPartitionOffset =
                Topic(record.topic).withPartition(record.kafkaPartition.intValue).withOffset(Offset(record.kafkaOffset))

              val key = Option(record.key()) match {
                case Some(k) => ValueToSinkDataConverter(k, Option(record.keySchema()))
                case None    => NullSinkData(Option(record.keySchema()))
              }
              val messageValue = ValueToSinkDataConverter(record.value(), Option(record.valueSchema()))
              messageValue match {
                case NullSinkData(_) if writerManager.shouldSkipNullValues() =>
                  logger.debug(
                    "[{}] Skipping null value for tpo {}/{}/{}",
                    connectorTaskId.show,
                    topicPartitionOffset.topic,
                    topicPartitionOffset.partition,
                    topicPartitionOffset.offset,
                  )
                case _ => // all other sink data
                  val msgDetails = MessageDetail(
                    key     = key,
                    value   = messageValue,
                    headers = HeaderToSinkDataConverter(record),
                    TimestampUtils.parseTime(Option(record.timestamp()).map(_.toLong))(_ =>
                      logger.debug(
                        s"Record timestamp is invalid ${record.timestamp()}",
                      ),
                    ),
                    Topic(record.topic()),
                    record.kafkaPartition(),
                    Offset(record.kafkaOffset()),
                  )
                  handleErrors {
                    writerManager.write(topicPartitionOffset, msgDetails)
                  }
              }

          }

          if (records.isEmpty) {
            handleErrors(writerManager.commitFlushableWriters())
          }
        }
      }
      ()
    } { e =>
      if (logMetrics) {
        logger.info(s"[${connectorTaskId.show}] put records=${records.size()} took $e ms")
      }
    }

  override def preCommit(
    currentOffsets: util.Map[KafkaTopicPartition, OffsetAndMetadata],
  ): util.Map[KafkaTopicPartition, OffsetAndMetadata] = {
    def getDebugInfo(in: util.Map[KafkaTopicPartition, OffsetAndMetadata]): String =
      in.entrySet().asScala.toList.map(e =>
        e.getKey.topic() + "-" + e.getKey.partition() + ":" + e.getValue.offset(),
      ).mkString(";")

    logger.debug(s"[{}] preCommit with offsets={}", connectorTaskId.show, getDebugInfo(currentOffsets): Any)

    val topicPartitionOffsetTransformed: Map[TopicPartition, OffsetAndMetadata] =
      Option(currentOffsets).getOrElse(new util.HashMap())
        .asScala
        .map {
          topicPartToOffsetTuple: (KafkaTopicPartition, OffsetAndMetadata) =>
            (
              TopicPartition(topicPartToOffsetTuple._1),
              topicPartToOffsetTuple._2,
            )
        }
        .toMap

    val actualOffsets = writerManager
      .preCommit(topicPartitionOffsetTransformed)
      .map {
        case (topicPartition, offsetAndMetadata) =>
          (topicPartition.toKafka, offsetAndMetadata)
      }.asJava

    logger.debug(s"[{}] Returning latest written offsets={}", connectorTaskId.show, getDebugInfo(actualOffsets): Any)
    actualOffsets
  }

  override def open(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    val partitionsDebug = partitions.asScala.map(tp => s"${tp.topic()}-${tp.partition()}").mkString(",")
    logger.debug(s"[{}] Open partitions", connectorTaskId.show, partitionsDebug: Any)

    val topicPartitions = partitions.asScala
      .map(tp => TopicPartition(Topic(tp.topic), tp.partition))
      .toSet

    handleErrors(
      for {
        tpoMap <- indexManager.open(topicPartitions)
      } yield {
        tpoMap.foreach {
          case (topicPartition, offset) =>
            logger.debug(
              s"[${connectorTaskId.show}] Seeking to ${topicPartition.topic.value}-${topicPartition.partition}:${offset.map(_.value)}",
            )
            offset.foreach(o => context.offset(topicPartition.toKafka, o.value))
        }
      },
    )
  }

  /**
   * Whenever close is called, the topics and partitions assigned to this task
   * may be changing, eg, in a re-balance. Therefore, we must commit our open files
   * for those (topic,partitions) to ensure no records are lost.
   */
  override def close(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    logger.debug(
      "[{}] CloudSinkTask.close with {} partitions",
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

  def createClient(config: CC): Either[Throwable, CT]

  def createStorageInterface(connectorTaskId: ConnectorTaskId, config: C, cloudClient: CT): StorageInterface[MD]

  def convertPropsToConfig(connectorTaskId: ConnectorTaskId, props: Map[String, String]): Either[Throwable, C]

  private def createWriterMan(
    props: Map[String, String],
  ): Either[Throwable, (IndexManager, WriterManager[MD], C)] =
    for {
      config          <- convertPropsToConfig(connectorTaskId, props)
      s3Client        <- createClient(config.connectionConfig)
      storageInterface = createStorageInterface(connectorTaskId, config, s3Client)
      _               <- setRetryInterval(config)
      (indexManager, writerManager) <- Try(
        writerManagerCreator.from(config)(connectorTaskId, storageInterface),
      ).toEither
      _ <- initializeFromConfig(config)
    } yield {
      logMetrics = config.logMetrics
      (indexManager, writerManager, config)
    }

  private def initializeFromConfig(config: C): Either[Throwable, Unit] =
    Try(initialize(
      config.connectorRetryConfig.getRetryLimit,
      config.errorPolicy,
    )).toEither

  private def setRetryInterval(config: C): Either[Throwable, Unit] =
    Try {
      //if error policy is retry set retry interval
      config.errorPolicy match {
        case RetryErrorPolicy() => context.timeout(config.connectorRetryConfig.getRetryIntervalMillis)
        case _                  =>
      }
    }.toEither

}
