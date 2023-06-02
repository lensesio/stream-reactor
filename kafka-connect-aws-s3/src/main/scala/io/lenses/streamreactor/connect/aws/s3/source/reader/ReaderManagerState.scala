package io.lenses.streamreactor.connect.aws.s3.source.reader

import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionSearcherResponse

case class ReaderManagerState(
  partitionResponses: Seq[PartitionSearcherResponse],
  readerManagers:     Seq[ReaderManager],
)
