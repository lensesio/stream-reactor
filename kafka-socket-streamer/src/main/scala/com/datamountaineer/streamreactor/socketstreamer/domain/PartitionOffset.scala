package com.datamountaineer.streamreactor.socketstreamer.domain


case class PartitionOffset(partition: Int, offset: Option[Long])
