package com.datamountaineer.streamreactor.temp

trait ThreadPoolSettings extends BaseSettings{
  val threadPoolConstant: String

  def getThreadPoolSize: Int = {
    val threads = getInt(threadPoolConstant)
    if (threads <= 0) 4 * Runtime.getRuntime.availableProcessors()
    else threads
  }

}
