
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

import com.datamountaineer.streamreactor.common.errors.{ErrorHandler, RetryErrorPolicy}
import com.datamountaineer.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.aws.s3.auth.{AuthResources, JCloudsS3ContextCreator}
import io.lenses.streamreactor.connect.aws.s3.config.{S3Config, S3ConfigDefBuilder}
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.ThrowableEither.toJavaThrowableConverter
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.sink.conversion.{HeaderToStringConverter, ValueToSinkDataConverter}
import io.lenses.streamreactor.connect.aws.s3.storage.{MultipartBlobStoreStorageInterface, StorageInterface}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import java.util
import scala.collection.JavaConverters._
import scala.util.Try

class S3SinkTask extends SinkTask with ErrorHandler {

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  private var writerManager: S3WriterManager = _

  private var sinkName: String = _

  override def version(): String = manifest.version()

  override def start(props: util.Map[String, String]): Unit = {
    sinkName = getSinkName(props).getOrElse("MissingSinkName")

    logger.debug(s"[{}] S3SinkTask.start", sinkName)

    val errOrWriterMan = for {
      config <- S3SinkConfig(S3ConfigDefBuilder(getSinkName(props), propsFromContext(props)))
      jCloudsAuth <- new AuthResources(config.s3Config).jClouds
      storageInterface <- Try {new MultipartBlobStoreStorageInterface(sinkName, jCloudsAuth)}.toEither
      _ <- Try {setErrorRetryInterval(config.s3Config)} .toEither
      writerManager <- Try {S3WriterManager.from(config, sinkName)(storageInterface)}.toEither
      _ <- Try(initialize(
        config.s3Config.connectorRetryConfig.numberOfRetries,
        config.s3Config.errorPolicy,
      )).toEither
    } yield writerManager

    errOrWriterMan match {
      case Left(error: String) => throw new IllegalStateException(error)
      case Left(error: Throwable) => throw error
      case Right(writerMan) =>
        writerManager = writerMan
    }

  }

  private def getSinkName(props: util.Map[String, String]) = {
    Option(props.get("name")).filter(_.trim.nonEmpty)
  }

  private def propsFromContext(props: util.Map[String, String]): util.Map[String, String] = {
    Option(context)
      .flatMap(c => Option(c.configs()))
      .filter(_.isEmpty == false)
      .getOrElse(props)
  }

  private def setErrorRetryInterval(s3Config: S3Config): Unit = {
    //if error policy is retry set retry interval
    s3Config.errorPolicy match {
      case RetryErrorPolicy() => context.timeout(s3Config.connectorRetryConfig.errorRetryInterval)
      case _ =>
    }
  }

  case class TopicAndPartition(topic: String, partition: Int) {
    override def toString: String = s"$topic-$partition"
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

  def cleanUpAndRollBack(topicPartitions: Set[TopicPartition]): Unit = {
    context.offset(
      topicPartitions.map {
        tp => (tp.toKafka, long2Long(writerManager.getLastCommittedOffset(tp).value))
      }.toMap.asJava
    )
    topicPartitions.foreach(writerManager.cleanUp)
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {

    handleTry {
      Try {
        val recordsStats = buildLogForRecords(records.asScala)
          .toList.sortBy(_._1).map { case (k, v) => s"$k=$v" }.mkString(";")

        logger.debug(s"[$sinkName] put records=${records.size()} stats=$recordsStats")

        // a failure in catchUp will prevent the processing of further records
        writerManager.catchUp().toThrowable(sinkName)

        records.asScala.foreach {
          record =>
            writerManager.write(
              Topic(record.topic).withPartition(record.kafkaPartition.intValue).withOffset(record.kafkaOffset),
              MessageDetail(
                keySinkData = Option(record.key()).fold(Option.empty[SinkData])(key => Option(ValueToSinkDataConverter(key, Option(record.keySchema())))),
                valueSinkData = ValueToSinkDataConverter(record.value(), Option(record.valueSchema())),
                headers = HeaderToStringConverter(record)
              )
            ) match {
              case Left(processorException: ProcessorException) =>
                logger.error(s"[$sinkName] ProcessorException encountered (recoverable), ${processorException.getMessage}")
                throw processorException
              case Left(commitException: BatchCommitException) =>
                logger.error(s"[$sinkName] BatchCommitException encountered (non-recoverable), ${commitException.getMessage}")
                cleanUpAndRollBack(commitException.commitExceptions.keys.map(_.topicPartition).toSet)
                // no point in any further processing
                return
              case Left(ex: SinkException) =>
                logger.error(s"[$sinkName] Other SinkException encountered (recoverable, but unexpected), ${ex.getMessage}")
                throw ex
              case Right(_) =>
            }
        }

        if (records.isEmpty) {
          val commitResult = writerManager.enqueueCommitAllWritersIfFlushRequired()
          commitResult match {
            case Left(commitException: BatchCommitException) =>
              logger.error(s"[$sinkName] BatchCommitException encountered (non-recoverable), ${commitException.getMessage}")
              cleanUpAndRollBack(commitException.commitExceptions.keys.map(_.topicPartition).toSet)
              // no point in any further processing
              return
            case Right(_) =>
          }
        }

        writerManager.catchUp().toThrowable(sinkName)
      }
    }

  }

  override def preCommit(currentOffsets: util.Map[KafkaTopicPartition, OffsetAndMetadata]): util.Map[KafkaTopicPartition, OffsetAndMetadata] = {
    def getDebugInfo(in: util.Map[KafkaTopicPartition, OffsetAndMetadata]): String = {
      in.entrySet().asScala.toList.map(e => e.getKey.topic() + "-" + e.getKey.partition() + ":" + e.getValue.offset()).mkString(";")
    }

    logger.debug(s"[{}] preCommit with offsets={}", sinkName, getDebugInfo(currentOffsets): Any)

    val topicPartitionOffsetTransformed: Map[TopicPartition, OffsetAndMetadata] = Option(currentOffsets).getOrElse(new util.HashMap())
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

    val topicPartitions = partitions.asScala
      .map(tp => TopicPartition(Topic(tp.topic), tp.partition))
      .toSet

    val throwableOrUnit = for {
      tpoMap <- writerManager.open(topicPartitions)
    } yield {
      tpoMap.foreach {
        case (topicPartition, offset) =>
          logger.debug(s"[$sinkName] Seeking to ${topicPartition.topic.value}-${topicPartition.partition}:${offset.value}")
          context.offset(topicPartition.toKafka, offset.value)
      }
    }
    throwableOrUnit.toThrowable(sinkName)

  }

  /**
    * Whenever close is called, the topics and partitions assigned to this task
    * may be changing, eg, in a re-balance. Therefore, we must commit our open files
    * for those (topic,partitions) to ensure no records are lost.
    */
  override def close(partitions: util.Collection[KafkaTopicPartition]): Unit = {
    logger.debug(s"[{}] S3SinkTask.close with {} partitions", sinkName, partitions.size())

    Option(writerManager).foreach(_.close())
  }

  override def stop(): Unit = {
    logger.debug(s"[{}] Stop")

    Option(writerManager).foreach(_.close())
    writerManager = null
  }

}
