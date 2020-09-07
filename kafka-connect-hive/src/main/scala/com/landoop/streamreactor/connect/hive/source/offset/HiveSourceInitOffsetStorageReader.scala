package com.landoop.streamreactor.connect.hive.source.offset

import com.landoop.streamreactor.connect.hive.source.{SourceOffset, SourcePartition, fromSourcePartition, toSourceOffset}
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConverters._
import scala.util.Try

class HiveSourceInitOffsetStorageReader(reader: OffsetStorageReader) extends HiveOffsetStorageReader {
  def offset(partition: SourcePartition): Option[SourceOffset] = {
    val offsetMap = reader.offset(fromSourcePartition(partition).asJava)
    Try(toSourceOffset(offsetMap.asScala.toMap)).toOption
  }
}
