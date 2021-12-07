package com.landoop.streamreactor.connect.hive.source.offset

import java.util

import com.landoop.streamreactor.connect.hive.source.{SourceOffset, SourcePartition, fromSourceOffset, toSourcePartition}
import org.apache.kafka.connect.storage.OffsetStorageReader


class MockOffsetStorageReader(map: Map[SourcePartition, SourceOffset]) extends OffsetStorageReader {

  override def offsets[T](partitions: util.Collection[util.Map[String, T]]): util.Map[util.Map[String, T], util.Map[String, AnyRef]] = ???

  override def offset[T](partition: util.Map[String, T]): util.Map[String, AnyRef] = {
    val sourcePartition = toSourcePartition(partition.asScala.toMap.mapValues(_.toString))
    map.get(sourcePartition).map(fromSourceOffset).map(_.asJava).orNull
  }
}
