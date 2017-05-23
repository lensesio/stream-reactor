/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.temp.traits

trait ThreadPoolSettings extends BaseSettings{
  val threadPoolConstant: String

  def getThreadPoolSize: Int = {
    val threads = getInt(threadPoolConstant)
    if (threads <= 0) 4 * Runtime.getRuntime.availableProcessors()
    else threads
  }

}
