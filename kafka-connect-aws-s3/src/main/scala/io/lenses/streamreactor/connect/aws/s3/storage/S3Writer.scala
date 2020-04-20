package io.lenses.streamreactor.connect.aws.s3.storage

import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.sink.{CommitContext, CommitPolicy, S3FileNamingStrategy}
import io.lenses.streamreactor.connect.aws.s3.{BucketAndPrefix, Offset, TopicPartition, TopicPartitionOffset}
import org.apache.kafka.connect.data.{Schema, Struct}

trait S3Writer {
  def commitChecks(): Option[TopicPartitionOffset]

  def close(): Unit

  def write(struct: Struct, tpo: TopicPartitionOffset): Unit

  def getCommittedOffset: Option[Offset]
}


case class S3WriterState(
                          topicPartition: TopicPartition,
                          offset: Offset,
                          committedOffset: Option[Offset],
                          createdTimestamp: Long = System.currentTimeMillis(),
                          recordCount: Long = 0,
                          lastKnownFileSize: Long = 0,
                          lastKnownSchema: Option[Schema] = None
                        )


class S3WriterImpl(
                    bucketAndPrefix: BucketAndPrefix,
                    commitPolicy: CommitPolicy,
                    formatWriterFn: TopicPartition => S3FormatWriter,
                    fileNamingStrategy: S3FileNamingStrategy,
                  )(implicit storageInterface: StorageInterface) extends S3Writer {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private var internalState: S3WriterState = _

  private var formatWriter: S3FormatWriter = _

  override def write(struct: Struct, tpo: TopicPartitionOffset): Unit = {

    if (formatWriter == null) {
      formatWriter = formatWriterFn(tpo.toTopicPartition)
    }

    logger.debug(s"S3Writer.write: Internal state: $internalState")

    if (shouldRollover(struct)) commit()

    if (internalState == null) {
      internalState = S3WriterState(
        tpo.toTopicPartition,
        tpo.offset,
        None,
        createdTimestamp = System.currentTimeMillis()
      )
    }

    // appends to output stream
    formatWriter.write(struct, tpo.topic)

    internalState = internalState.copy(
      lastKnownFileSize = formatWriter.getPointer,
      lastKnownSchema = Option(struct.schema()),
      recordCount = internalState.recordCount + 1,
      offset = tpo.offset
    )

  }

  private def shouldRollover(struct: Struct) = {
    rolloverOnSchemaChange &&
      internalState != null &&
      schemaHasChanged(struct)
  }

  private def schemaHasChanged(struct: Struct) = {
    internalState.lastKnownSchema.isEmpty ||
      internalState.lastKnownSchema.get != struct.schema()
  }

  private def rolloverOnSchemaChange = {
    formatWriter != null &&
      formatWriter.rolloverFileOnSchemaChange()
  }

  private def commit(): TopicPartitionOffset = {
    logger.debug(s"S3Writer - Commit")
    val topicPartitionOffset = TopicPartitionOffset(
      internalState.topicPartition.topic,
      internalState.topicPartition.partition,
      internalState.offset)

    formatWriter.close()
    if(formatWriter.getOutstandingRename) {
      renameFile(topicPartitionOffset)
    }

    resetState(topicPartitionOffset)

    topicPartitionOffset
  }

  def renameFile(topicPartitionOffset: TopicPartitionOffset): Unit = {
    val originalFilename = fileNamingStrategy.stagingFilename(
      bucketAndPrefix,
      topicPartitionOffset.toTopicPartition
    )
    val finalFilename = fileNamingStrategy.finalFilename(
      bucketAndPrefix,
      topicPartitionOffset
    )
    storageInterface.rename(originalFilename, finalFilename)
  }

  def resetState(topicPartitionOffset: TopicPartitionOffset): Unit = {
    logger.debug(s"S3Writer.resetState: Resetting state: $internalState")

    internalState = internalState.copy(
      committedOffset = Some(topicPartitionOffset.offset),
      lastKnownFileSize = 0.toLong,
      recordCount = 0.toLong
    )

    formatWriter = formatWriterFn(topicPartitionOffset.toTopicPartition)

    logger.debug(s"S3Writer.resetState: New internal state: $internalState")
  }

  override def close(): Unit = storageInterface.close()

  override def getCommittedOffset: Option[Offset] = internalState.committedOffset

  override def commitChecks: Option[TopicPartitionOffset] = {

    val commitContext = CommitContext(
      TopicPartitionOffset(internalState.topicPartition.topic, internalState.topicPartition.partition, internalState.offset),
      internalState.recordCount,
      internalState.lastKnownFileSize,
      internalState.createdTimestamp
    )
    if (commitPolicy.shouldFlush(commitContext)) {

      logger.debug(s"S3Writer - Flushing according to commit policy with commitContext $commitContext, State: $internalState")

      Some(commit())
    } else {
      logger.debug(s"S3Writer - Skipped Flushing according to commit policy with commitContext $commitContext, State: $internalState")

      None
    }
  }

}
