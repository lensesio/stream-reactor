package com.datamountaineer.streamreactor.temp

trait BatchSizeSettings extends BaseSettings {
  val batchSizeConstant: String

  def getBatchSize: Int = getInt(batchSizeConstant)
}
