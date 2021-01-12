package io.lenses.streamreactor.connect.aws.s3.sink.commit

import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.sink.{IndexFileName, PartitionedS3IndexNamingStrategy, S3FileNamingStrategy}
import io.lenses.streamreactor.connect.aws.s3.storage.Storage
import org.apache.kafka.connect.errors.ConnectException

import scala.io.Source
import scala.util.control.NonFatal

class Gen2Committer(storage: Storage,
                    fileNamingStrategyFn: Topic => S3FileNamingStrategy,
                    bucketAndPrefixFn: Topic => BucketAndPrefix) extends Committer {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private val indexNamingStrategy = new PartitionedS3IndexNamingStrategy

  /**
   * Commit records to S3 bucket.
   *
   * @param bucketAndPrefix      the bucket.
   * @param topicPartitionOffset the offset.
   * @param partitionValues      the partition values.
   */
  override def commit(bucketAndPrefix: BucketAndPrefix,
                      topicPartitionOffset: TopicPartitionOffset,
                      partitionValues: Map[PartitionField, String]): Unit = {
    val fileNamingStrategy = fileNamingStrategyFn(topicPartitionOffset.topic)
    val originalFilename = fileNamingStrategy.stagingFilename(
      bucketAndPrefix,
      topicPartitionOffset.toTopicPartition,
      partitionValues
    )
    val finalFilename = fileNamingStrategy.finalFilename(
      bucketAndPrefix,
      topicPartitionOffset,
      partitionValues
    )

    val topicPartition = topicPartitionOffset.toTopicPartition
    val currentIndex = findIndexPath(topicPartition)
    val newIndex = indexNamingStrategy.indexFilename(bucketAndPrefix.bucket, topicPartitionOffset)

    // create new index
    storage.putBlob(newIndex, finalFilename.path)
    // commit
    storage.rename(originalFilename, finalFilename)
    // remove current index
    currentIndex.foreach(indexPath => storage.remove(indexPath))
  }

  /**
   * Find latest offset per partition.
   *
   * @param partitions the set of topic partitions
   * @return the offset per partition <code>Map</code>.
   */
  override def latest(partitions: Set[TopicPartition]): Map[TopicPartition, Offset] = {
    partitions.collect {
      case topicPartition: TopicPartition =>
        findIndexPath(topicPartition)
          .flatMap(bucketAndPath => IndexFileName.from(bucketAndPath.path, indexNamingStrategy))
          .map(file => TopicPartitionOffset(file.topic, file.partition, file.offset)) match {
          case Some(topicPartitionOffset) => Some(topicPartition, topicPartitionOffset.offset)
          case None => None
        }
    }.flatten.toMap
  }

  /**
   * Find latest index path for a partition.
   *
   * @param topicPartition the topic partition
   * @return the bucket and path of latest index.
   */
  private def findIndexPath(topicPartition: TopicPartition): Option[BucketAndPath] = {
    try {
      val bucket = bucketAndPrefixFn(topicPartition.topic).bucket
      val indexBucketAndPath = indexNamingStrategy.indexPath(bucket, topicPartition)

      val listOfIndexes = storage.list(indexBucketAndPath)

      val orderedIndexes = listOfIndexes.flatMap(IndexFileName.from(_, indexNamingStrategy))
        .map(file => TopicPartitionOffset(file.topic, file.partition, file.offset))
        .sortBy(_.offset)(Ordering[Offset])

      orderedIndexes.size match {
        case 0 => None
        case 1 => Some(BucketAndPath(bucket, listOfIndexes.head))
        case 2 => handleIndexConflict(bucket, orderedIndexes)
        case _ => throw new IllegalStateException("Corrupted index state")
      }
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error finding index for $topicPartition", e)
        throw e
    }
  }

  /**
   * In case multiple indexes are available try to resolve conflict
   * by removing the non-committed index.
   *
   * @param bucket         the bucket
   * @param orderedIndexes the ordered list of indexes.
   * @return the latest committed file index
   */
  private def handleIndexConflict(bucket: String, orderedIndexes: Vector[TopicPartitionOffset]) = {
    logger.warn(s"Found multiple index files: $orderedIndexes")

    val indexPrevious = indexNamingStrategy.indexFilename(bucket, orderedIndexes.head)
    val indexLast = indexNamingStrategy.indexFilename(bucket, orderedIndexes.last)

    val committedFileName = Source.fromInputStream(storage.getBlob(indexLast)).mkString
    if (storage.pathExists(BucketAndPath(bucket, committedFileName))) {
      storage.remove(indexPrevious)
      Some(indexLast)
    } else {
      storage.remove(indexLast)
      Some(indexPrevious)
    }
  }
}

object Gen2Committer {
  def from(config: S3SinkConfig, storage: Storage): Gen2Committer = {
    val bucketAndPrefixFn: Topic => BucketAndPrefix = topic => config.bucketOptions.find(_.sourceTopic == topic.value)
      .getOrElse(throw new ConnectException(s"No bucket config for $topic")).bucketAndPrefix

    val fileNamingStrategyFn: Topic => S3FileNamingStrategy = topic => config.bucketOptions
      .find(_.sourceTopic == topic.value) match {
      case Some(bucketOptions) => bucketOptions.fileNamingStrategy
      case None => throw new IllegalArgumentException("Can't find fileNamingStrategy in config")
    }

    new Gen2Committer(storage, fileNamingStrategyFn, bucketAndPrefixFn)
  }
}
