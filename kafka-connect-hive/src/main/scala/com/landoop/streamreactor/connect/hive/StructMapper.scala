package com.landoop.streamreactor.connect.hive

import org.apache.kafka.connect.data.Struct

/**
  * Maps between an input record and an output record.
  */
trait StructMapper {
  def map(input: Struct): Struct
}