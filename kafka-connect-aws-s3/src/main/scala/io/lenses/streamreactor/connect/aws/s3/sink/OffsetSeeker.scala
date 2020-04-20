package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import io.lenses.streamreactor.connect.aws.s3.{BucketAndPrefix, TopicPartitionOffset}

import scala.util.control.NonFatal

/**
  * The [[OffsetSeeker]] is responsible for querying the [[StorageInterface]] to
  * retrieve current offset information from a container.
  *
  * @param fileNamingStrategy we need the policy so we can match on this.
  */
class OffsetSeeker(fileNamingStrategy: S3FileNamingStrategy) {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  def seek(bucketAndPrefix: BucketAndPrefix)(implicit storageInterface: StorageInterface): Set[TopicPartitionOffset] = {
    try {

      // the path may not have been created, in which case we have no offsets defined
      if (storageInterface.pathExists(bucketAndPrefix)) {

        val listOfFilesInBucket = storageInterface.list(bucketAndPrefix)

        listOfFilesInBucket.collect {
          case CommittedFileName(_, topic, partition, end, format)
            if format == fileNamingStrategy.getFormat =>
            TopicPartitionOffset(topic, partition, end)
        }.groupBy(_.toTopicPartition).map { case (tp, tpo) =>
          tp.withOffset(tpo.maxBy(_.offset.value).offset)
        }.toSet

      } else {
        Set.empty
      }

    } catch {
      case NonFatal(e) =>
        logger.error(s"Error seeking bucket/prefix $bucketAndPrefix")
        throw e
    }

  }

}

