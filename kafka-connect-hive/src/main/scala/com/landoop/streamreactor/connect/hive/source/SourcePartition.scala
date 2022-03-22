package com.landoop.streamreactor.connect.hive.source

import com.landoop.streamreactor.connect.hive.{DatabaseName, TableName, Topic}
import org.apache.hadoop.fs.Path


  case class SourcePartition(db: DatabaseName, tableName: TableName, topic: Topic, path: Path)

  case class SourceOffset(rowNumber: Int)

object SourcePartition {

  def fromSourcePartition(partition: SourcePartition): Map[String, AnyRef] =
    Map[String, AnyRef](
      "db" -> partition.db.value,
      "table" -> partition.tableName.value,
      "topic" -> partition.topic.value,
      "path" -> partition.path.toString
    )

  def toSourcePartition(map: Map[String, AnyRef]): SourcePartition =
    SourcePartition(DatabaseName(map("db").toString), TableName(map("table").toString), Topic(map("topic").toString), new Path(map("path").toString))

  def fromSourceOffset(offset: SourceOffset): Map[String, AnyRef] =
    Map("rownum" -> offset.rowNumber.toString)

  def toSourceOffset(map: Map[String, AnyRef]): SourceOffset =
    SourceOffset(map("rownum").toString.toInt)
}