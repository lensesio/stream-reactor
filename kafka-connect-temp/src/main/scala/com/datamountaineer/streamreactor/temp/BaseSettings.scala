package com.datamountaineer.streamreactor.temp

trait BaseSettings {
  def getString(key: String): String

  def getInt(key: String): Integer

  def getBoolean(key: String): java.lang.Boolean
}
