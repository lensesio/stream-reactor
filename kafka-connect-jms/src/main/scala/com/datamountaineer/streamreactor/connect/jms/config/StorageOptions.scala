package com.datamountaineer.streamreactor.connect.jms.config

import com.datamountaineer.kcql.Kcql

import scala.jdk.CollectionConverters.MapHasAsScala

case class StorageOptions(storedAs: String, storedAsProperties: Map[String, String])

object StorageOptions {
  def apply(r: Kcql): StorageOptions =
    StorageOptions(
      r.getStoredAs,
      r.getStoredAsParameters.asScala.toMap,
    )
}
