
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

import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model.Offset.orderingByOffsetValue
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.model.location.{RemoteS3PathLocation, RemoteS3RootLocation}
import io.lenses.streamreactor.connect.aws.s3.processing.{BlockingQueueProcessor, ProcessorManager}
import io.lenses.streamreactor.connect.aws.s3.sink.config.{S3SinkConfig, SinkBucketOptions}
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.data.Schema

import scala.collection.mutable
import scala.util.Try

case class MapKey(topicPartition: TopicPartition, bucketAndPath: RemoteS3PathLocation)

/**
  * Manages the lifecycle of [[S3Writer]] instances.
  *
  * A given sink may be writing to multiple locations (partitions), and therefore
  * it is convenient to extract this to another class.
  *
  * This class is not thread safe as it is not designed to be shared between concurrent
  * sinks, since file handles cannot be safely shared without considerable overhead.
  */
class S3WriterManager(sinkName: String,
                      formatWriterFn: (TopicPartitionOffset, Map[PartitionField, String], Offset => () => Unit) => Either[ProcessorException, S3FormatWriter],
                      commitPolicyFn: Topic => Either[ProcessorException, CommitPolicy],
                      bucketAndPrefixFn: Topic => Either[ProcessorException, RemoteS3RootLocation],
                      fileNamingStrategyFn: Topic => Either[ProcessorException, S3FileNamingStrategy],
                     )
                     (implicit storageInterface: StorageInterface,
                      processorManager: ProcessorManager
                     ) extends StrictLogging {


  private val initialOpenOffsets = mutable.Map.empty[TopicPartition, Offset]

  private val writers = mutable.Map.empty[MapKey, S3Writer]

  def enqueueCommitAllWritersIfFlushRequired(): Either[BatchCommitException, Unit] = {
    if (writers.values.exists(_.shouldFlush)) {
      enqueueCommitAllWriters()
    } else {
      ().asRight
    }
  }

  private def enqueueCommitAllWriters(): Either[BatchCommitException, Unit] = {
    logger.debug(s"[{}] Received call to S3WriterManager.commit", sinkName)
    enqueueCommitWriters(_ => true)
  }

  private def enqueueCommitTopicPartitionWriters(topicPartition: TopicPartition): Either[BatchCommitException, Unit] = {
    enqueueCommitWriters(mapKey => mapKey.topicPartition == topicPartition)
  }

  private def enqueueCommitWriters(keyFilterFn: MapKey => Boolean): Either[BatchCommitException, Unit] = {

    logger.debug(s"[{}] Received call to S3WriterManager.commit", sinkName)
    val errorsSet = writers
      .filterKeys(keyFilterFn)
      .mapValues(_.enqueueCommit)
      .collect {
        case (key: MapKey, Left(exception: Exception)) => (key, exception)
      }.toMap

    if (errorsSet.nonEmpty) BatchCommitException(errorsSet).asLeft else ().asRight
  }

  def open(partitions: Set[TopicPartition]): Either[ProcessorException, Map[TopicPartition, Offset]] = {
    logger.debug(s"[{}] Received call to S3WriterManager.open", sinkName)

    val (errors, offsets) = partitions.collect {
      case topicPartition: TopicPartition =>
        for {
          fileNamingStrategy <- fileNamingStrategyFn(topicPartition.topic)
          bucketAndPrefix <- bucketAndPrefixFn(topicPartition.topic)
          topicPartitionPrefix <- Try(fileNamingStrategy.topicPartitionPrefix(bucketAndPrefix, topicPartition)).toEither
        } yield {
          new OffsetSeeker(fileNamingStrategy)
            .seek(topicPartitionPrefix)
            .find(_.toTopicPartition == topicPartition)
        }
    }.partition(_.isLeft)

    if (errors.nonEmpty) {
      ProcessorException(
        errors.collect {
          case Left(exception: Exception) => exception
        }
      ).asLeft
    } else {

      offsets
        .collect {
          case Right(Some(offset)) => offset.toTopicPartitionOffsetTuple
        }
        .map {
          case (tp, o) => initialOpenOffsets.put(tp, o)
            (tp, o)
        }
        .toMap
        .asRight
    }
  }

  def close(): Unit = {
    logger.debug(s"[{}] Received call to S3WriterManager.close", sinkName)
    writers.values.foreach(_.close())
  }

  def catchUp(): Either[ProcessorException, Unit] = processorManager.catchUp()

  def write(topicPartitionOffset: TopicPartitionOffset, messageDetail: MessageDetail): Either[SinkException, Unit] = {

    logger.debug(s"[$sinkName] Received call to S3WriterManager.write for ${topicPartitionOffset.topic}-${topicPartitionOffset.partition}:${topicPartitionOffset.offset}")

    writer(topicPartitionOffset.toTopicPartition, messageDetail) match {
      case Left(throwable) => throwable.asLeft
      case Right(writer) =>
        if (writer.shouldSkip(topicPartitionOffset.offset)) {
          ().asRight
        } else {

          // commitException can not be recovered from
          rollOverTopicPartitionWriters(writer, topicPartitionOffset.toTopicPartition, messageDetail) match {
            case Left(exception) => return exception.asLeft
            case Right(_) =>
          }

          // a processErr can potentially be recovered from in the next iteration.  Can be due to network problems, for
          // example
          val processErr = for {
            _ <- writer.enqueueWrite(messageDetail, topicPartitionOffset)
            procRes <- writer.process()
          } yield procRes

          if (writer.shouldFlush) {
            // a commitErr cannot be recovered from in the next iteration and we must revert back to the lastCommittedOffset
            enqueueCommitTopicPartitionWriters(topicPartitionOffset.toTopicPartition) match {
              case Left(batchCommitException: BatchCommitException) => return batchCommitException.asLeft
              case Right(_) =>
            }
          }

          processErr
        }
    }


  }

  private def rollOverTopicPartitionWriters(
                                             s3Writer: S3Writer,
                                             topicPartition: TopicPartition,
                                             messageDetail: MessageDetail
                                           ): Either[BatchCommitException, Unit] = {
    messageDetail.valueSinkData.schema() match {
      case Some(value: Schema) if s3Writer.shouldRollover(value) =>
        enqueueCommitTopicPartitionWriters(topicPartition) match {
          case Left(err) => err.asLeft
          case Right(_) => ().asRight
        }
      case _ =>
        ().asRight
    }
  }

  def processPartitionValues(
                              messageDetail: MessageDetail,
                              fileNamingStrategy: S3FileNamingStrategy,
                              topicPartition: TopicPartition
                            ): Either[ProcessorException, Map[PartitionField, String]] = {
    if (fileNamingStrategy.shouldProcessPartitionValues) {
      fileNamingStrategy.processPartitionValues(messageDetail, topicPartition)
    } else {
      Map.empty[PartitionField, String].asRight
    }
  }

  /**
    * Returns a writer that can write records for a particular topic and partition.
    * The writer will create a file inside the given directory if there is no open writer.
    */
  private def writer(topicPartition: TopicPartition, messageDetail: MessageDetail): Either[SinkException, S3Writer] = {
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition.topic)
      fileNamingStrategy <- fileNamingStrategyFn(topicPartition.topic)
      partitionValues <- processPartitionValues(messageDetail, fileNamingStrategy, topicPartition)
      stagingFilename <- fileNamingStrategy.stagingFilename(bucketAndPrefix, topicPartition, partitionValues)
      processor = processorManager.processor(topicPartition, stagingFilename)
    } yield writers.getOrElseUpdate(
      MapKey(topicPartition, stagingFilename), createWriter(bucketAndPrefix, topicPartition, partitionValues, processor) match {
        case Left(ex) => return ex.asLeft[S3Writer]
        case Right(value) => value
      }
    )
  }

  private def createWriter(bucketAndPrefix: RemoteS3RootLocation, topicPartition: TopicPartition, partitionValues: Map[PartitionField, String], processor: BlockingQueueProcessor): Either[ProcessorException, S3Writer] = {
    logger.debug(s"[$sinkName] Creating new writer for bucketAndPrefix:$bucketAndPrefix")
    for {
      commitPolicy <- commitPolicyFn(topicPartition.topic)
      fileNamingStrategy <- fileNamingStrategyFn(topicPartition.topic)
    } yield {
      new S3Writer(
        sinkName,
        bucketAndPrefix,
        commitPolicy,
        formatWriterFn,
        fileNamingStrategy,
        partitionValues,
        processor
      )
    }
  }

  def preCommit(currentOffsets: Map[TopicPartition, OffsetAndMetadata]): Map[TopicPartition, OffsetAndMetadata] = {
    currentOffsets
      .collect {
        case (topicPartition, offsetAndMetadata) =>
          val candidateWriters = writers
            .filter {
              case (key, writer) => key.topicPartition == topicPartition && writer.getCommittedOffset.nonEmpty
            }
            .values
          if (candidateWriters.isEmpty) {
            None
          } else {
            Some(
              topicPartition,
              createOffsetAndMetadata(offsetAndMetadata, candidateWriters
                .maxBy(_.getCommittedOffset))
            )
          }

      }.flatten.toMap
  }

  private def createOffsetAndMetadata(offsetAndMetadata: OffsetAndMetadata, writer: S3Writer) = {
    new OffsetAndMetadata(
      writer.getCommittedOffset.get.value,
      offsetAndMetadata.leaderEpoch(),
      offsetAndMetadata.metadata()
    )
  }

  def cleanUp(topicPartition: TopicPartition): Unit = {
    writers
      .filterKeys(mapKey => mapKey
        .topicPartition == topicPartition)
      .keys
      .foreach({
        writers.remove
      })

    processorManager.cleanUp(topicPartition: TopicPartition)
  }

  def getLastCommittedOffset(topicPartition: TopicPartition): Offset = {
    val offsets = writers.filterKeys(mapKey => mapKey.topicPartition == topicPartition)
      .values
      .flatMap(_.getCommittedOffset)
    if (offsets.isEmpty) {
      initialOpenOffsets(topicPartition)
    } else {
      offsets.max
    }
  }

}

object S3WriterManager extends LazyLogging {

  def from(config: S3SinkConfig, sinkName: String)
          (implicit storageInterface: StorageInterface): S3WriterManager = {

    implicit val processorMan: ProcessorManager = new ProcessorManager()

    val bucketAndPrefixFn: Topic => Either[ProcessorException, RemoteS3RootLocation] = topic => {
      bucketOptsForTopic(config, topic)
        .fold(ProcessorException(s"No bucket config for $topic").asLeft[RemoteS3RootLocation])(_.bucketAndPrefix.asRight[ProcessorException])
    }

    val commitPolicyFn: Topic => Either[ProcessorException, CommitPolicy] = topic => bucketOptsForTopic(config, topic) match {
      case Some(bucketOptions) => bucketOptions.commitPolicy.asRight
      case None => ProcessorException("Can't find commitPolicy in config").asLeft
    }

    val fileNamingStrategyFn: Topic => Either[ProcessorException, S3FileNamingStrategy] = topic =>
      bucketOptsForTopic(config, topic) match {
        case Some(bucketOptions) => bucketOptions.fileNamingStrategy.asRight
        case None => ProcessorException("Can't find fileNamingStrategy in config").asLeft
      }

    val processorFn: (TopicPartition, RemoteS3PathLocation) => BlockingQueueProcessor = processorMan.processor

    val formatWriterFn: (TopicPartitionOffset, Map[PartitionField, String], Offset => () => Unit) => Either[ProcessorException, S3FormatWriter] = (topicPartitionInitialOffset, partitionValues, updateOffsetFn) =>
      bucketOptsForTopic(config, topicPartitionInitialOffset.topic) match {
        case Some(bucketOptions) =>
          val stagingFilename = for {
            fileNamingStrategy <- fileNamingStrategyFn(topicPartitionInitialOffset.topic)
            stagingFileName <- fileNamingStrategy.stagingFilename(bucketOptions.bucketAndPrefix, topicPartitionInitialOffset.toTopicPartition, partitionValues)
          } yield stagingFileName
          stagingFilename match {
            case Left(ex) => ex.asLeft
            case Right(stagingFilename) => implicit val processor: BlockingQueueProcessor = processorFn(topicPartitionInitialOffset.toTopicPartition, stagingFilename)
              // a new tpProcessor is created for each unique location
              bucketOptions.writeMode.createFormatWriter(
                bucketOptions.formatSelection,
                stagingFilename,
                topicPartitionInitialOffset.offset,
                updateOffsetFn
              )
          }

        case None => throw new IllegalArgumentException("Can't find commitPolicy in config")
      }

    new S3WriterManager(
      sinkName,
      formatWriterFn,
      commitPolicyFn,
      bucketAndPrefixFn,
      fileNamingStrategyFn,
    )
  }

  private def bucketOptsForTopic(config: S3SinkConfig, topic: Topic): Option[SinkBucketOptions] = {
    config.bucketOptions.find(_.sourceTopic == topic.value)
  }

}
