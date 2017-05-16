package com.datamountaineer.streamreactor.temp

trait BaseSettings {
  val connectorPrefix: String

  def getString(key: String): String

  def getInt(key: String): Integer

  def getBoolean(key: String): java.lang.Boolean
}
