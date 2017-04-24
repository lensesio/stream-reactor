package com.datamountaineer.streamreactor.temp

trait AllowParallelizationSettings extends BaseSettings{
  val allowParallelConstant: String

  def getAllowParallel = getBoolean(allowParallelConstant)
}
