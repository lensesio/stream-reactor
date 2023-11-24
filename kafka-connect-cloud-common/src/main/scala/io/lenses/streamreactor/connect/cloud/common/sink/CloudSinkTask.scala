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
package io.lenses.streamreactor.connect.cloud.common.sink

import cats.implicits.toBifunctorOps
import cats.implicits.toShow
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskIdCreator
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.HeaderToStringConverter
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.ValueToSinkDataConverter
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.utils.MapUtils
import io.lenses.streamreactor.connect.cloud.common.utils.TimestampUtils
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Try

abstract class CloudSinkTask[SM <: FileMetadata](
  connectorPrefix:      String,
  sinkAsciiArtResource: String,
  manifest:             JarManifest,
)(
  implicit
  val cloudLocationValidator: CloudLocationValidator,
) extends SinkTask
    with ErrorHandler {
  private var writerManager:    WriterManager[SM] = _
  implicit var connectorTaskId: ConnectorTaskId   = _
  override def version():       String            = manifest.version()

  override def start(fallbackProps: util.Map[String, String]): Unit = {

    printAsciiHeader(manifest, sinkAsciiArtResource)

    new ConnectorTaskIdCreator(connectorPrefix).fromProps(fallbackProps) match {
      case Left(value)  => throw new IllegalArgumentException(value)
      case Right(value) => connectorTaskId = value
    }

    logger.debug(s"[{}] CloudSinkTask.start", connectorTaskId.show)

    val contextProps   = Option(context).flatMap(c => Option(c.configs())).map(_.asScala.toMap).getOrElse(Map.empty)
    val props          = MapUtils.mergeProps(contextProps, fallbackProps.asScala.toMap)
    val errOrWriterMan = createWriterMan(props)

    errOrWriterMan.leftMap(throw _).foreach(writerManager = _)
  }

  def createWriterMan(props: Map[String, String]): Either[Throwable, WriterManager[SM]]

  private def rollback(topicPartitions: Set[TopicPartition]): Unit =
    topicPartitions.foreach(writerManager.cleanUp)

  private def handleErrors(value: Either[SinkError, Unit]): Unit =
    value match {
      case Left(error: SinkError) =>
        if (error.rollBack()) {
          rollback(error.topicPartitions())
        }
        throw new IllegalStateException(error.message(), error.exception())
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

  override def put(records: util.Collection[SinkRecord]): Unit = {

    val _ = handleTry {
      Try {

        logger.debug(
          s"[${connectorTaskId.show}] put records=${records.size()} stats=${buildLogForRecords(records.asScala)
            .toList.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString(";")}",
        )

        // a failure in recommitPending will prevent the processing of further records
        handleErrors(writerManager.recommitPending())

        records.asScala.foreach {
          record =>
            val topicPartitionOffset =
              Topic(record.topic).withPartition(record.kafkaPartition.intValue).withOffset(Offset(record.kafkaOffset))

            val key = Option(record.key()) match {
              case Some(k) => ValueToSinkDataConverter(k, Option(record.keySchema()))
              case None    => NullSinkData(Option(record.keySchema()))
            }
            val msgDetails = MessageDetail(
              key     = key,
              value   = ValueToSinkDataConverter(record.value(), Option(record.valueSchema())),
              headers = HeaderToStringConverter(record),
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

        if (records.isEmpty) {
          handleErrors(writerManager.commitAllWritersIfFlushRequired())
        }
      }
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
        tpoMap <- writerManager.open(topicPartitions)
      } yield {
        tpoMap.foreach {
          case (topicPartition, offset) =>
            logger.debug(
              s"[${connectorTaskId.show}] Seeking to ${topicPartition.topic.value}-${topicPartition.partition}:${offset.value}",
            )
            context.offset(topicPartition.toKafka, offset.value)
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
}
