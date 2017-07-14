package com.datamountaineer.streamreactor.connect.cassandra.sink

import com.datamountaineer.kcql.Field

import scala.collection.JavaConversions._

object FieldConverter {
  def apply(field: Field): com.landoop.json.sql.Field = {
    com.landoop.json.sql.Field(
      field.getName,
      field.getAlias,
      Option(field.getParentFields).map(_.toVector).orNull)
  }
}
