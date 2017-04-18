package com.datamountaineer.streamreactor.temp

trait NumberRetriesSettings extends BaseSettings {
  val numberRetriesConstant: String

  def getNumberRetries: Int = getInt(numberRetriesConstant)
}
