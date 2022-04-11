package com.landoop.streamreactor.connect.hive.source.offset

import com.landoop.streamreactor.connect.hive.source.SourcePartition.{fromSourcePartition, toSourceOffset}
import com.landoop.streamreactor.connect.hive.source.{SourceOffset, SourcePartition}
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.jdk.CollectionConverters.{MapHasAsJava, MapHasAsScala}
import scala.util.Try

/**
  * Provides a reader for the Hive offsets from the context to be used on the Source initialisation
  * @param reader the reader provided by the context for retrieving the offsets
  */
class HiveSourceInitOffsetStorageReader(reader: OffsetStorageReader) extends HiveSourceOffsetStorageReader {

  def offset(partition: SourcePartition): Option[SourceOffset] = {
    val offsetMap = reader.offset(fromSourcePartition(partition).asJava)
    Try(toSourceOffset(offsetMap.asScala.toMap)).toOption
  }

}
