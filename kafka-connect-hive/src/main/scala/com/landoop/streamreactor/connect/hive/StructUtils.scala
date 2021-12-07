package com.landoop.streamreactor.connect.hive

import org.apache.kafka.connect.data.Struct


object StructUtils {
  def extractValues(struct: Struct): Vector[Any] = {
    struct.schema().fields().asScala.map(_.name).map(struct.get).toVector
  }
}
