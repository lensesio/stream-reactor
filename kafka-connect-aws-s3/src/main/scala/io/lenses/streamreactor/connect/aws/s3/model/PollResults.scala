package io.lenses.streamreactor.connect.aws.s3.model

import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import org.apache.kafka.connect.source.SourceRecord

case class PollResults(
  resultList:    Vector[_ <: SourceData],
  bucketAndPath: RemoteS3PathLocation,
  targetTopic:   String,
) {

  def toSourceRecordList: Vector[SourceRecord] =
    resultList.map(_.toSourceRecord(bucketAndPath, targetTopic))

}
