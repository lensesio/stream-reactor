package com.datamountaineer.streamreactor.temp

trait RetryIntervalSettings extends BaseSettings {
  val retryIntervalConstant: String

  def getRetryInterval: Int = getInt(retryIntervalConstant)
}
