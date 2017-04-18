package com.datamountaineer.streamreactor.temp

trait ThreadPoolSettings extends BaseSettings{
  val ThreadPoolConstant: String

  def getThreadPoolSize: Int = {
    val threads = getInt(ThreadPoolConstant)
    if (threads <= 0) 4 * Runtime.getRuntime.availableProcessors()
    else threads
  }

}
