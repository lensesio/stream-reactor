package com.datamountaineer.streamreactor.temp

trait DatabaseSettings extends BaseSettings {
  val databaseConstant: String

  def getDatabase: String = getString(databaseConstant)
}
