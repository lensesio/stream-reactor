package com.datamountaineer.streamreactor.temp
import java.lang

trait AllowParallelizationSettings extends BaseSettings{
  val allowParallelConstant: String

  def getAllowParallel: lang.Boolean = getBoolean(allowParallelConstant)
}
