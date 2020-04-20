package io.lenses.streamreactor.connect.aws.s3.storage

import io.lenses.streamreactor.connect.aws.s3.{BucketAndPath, BucketAndPrefix}
import org.jclouds.blobstore.domain.{MultipartPart, MultipartUpload}

case class MultiPartUploadState(
                                 upload: MultipartUpload,
                                 parts: Vector[MultipartPart]
                               )

trait StorageInterface {

  def initUpload(bucketAndPath: BucketAndPath): MultiPartUploadState

  def completeUpload(state: MultiPartUploadState): Unit

  def uploadPart(state: MultiPartUploadState, bytes: Array[Byte], size: Long): MultiPartUploadState

  def rename(originalFilename: BucketAndPath, newFilename: BucketAndPath): Unit

  def close(): Unit

  def pathExists(bucketAndPrefix: BucketAndPrefix): Boolean

  def list(bucketAndPrefix: BucketAndPrefix): List[String]

}

