package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive.{Offset, Topic}

import scala.util.Try

trait FilenamePolicy {
  val prefix: String
}

object DefaultFilenamePolicy extends FilenamePolicy {
  val prefix = "streamreactor"
}

object CommittedFileName {

  private val Regex = s"(.+)_(.+)_(\\d+)_(\\d+)_(\\d+)".r

  def unapply(filename: String): Option[(String, Topic, Int, Offset, Offset)] = {
    filename match {
      case Regex(prefix, topic, partition, start, end) =>
        Try((prefix, Topic(topic), partition.toInt, Offset(start.toLong), Offset(end.toLong))).toOption
      case _ => None
    }
  }
}