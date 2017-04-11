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

package com.datamountaineer.streamreactor.connect.hazelcast.config

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */
case class HazelCastConnectionConfig(group: String,
                                     members: Set[String],
                                     redo: Boolean = true,
                                     connectionAttempts: Int,
                                     connectionTimeouts: Long,
                                     pass : String,
                                     socketConfig: HazelCastSocketConfig)

case class HazelCastSocketConfig(keepAlive: Boolean = true,
                                 tcpNoDelay: Boolean = true,
                                 reuseAddress: Boolean = true,
                                 lingerSeconds: Int = 3,
                                 bufferSize: Int = 32)


object HazelCastConnectionConfig {
  def apply(config: HazelCastSinkConfig): HazelCastConnectionConfig = {
    val members = config.getList(HazelCastSinkConfigConstants.CLUSTER_SINK_MEMBERS).toSet
    val redo = true
    val connectionAttempts = config.getInt(HazelCastSinkConfigConstants.CONNECTION_RETRY_ATTEMPTS)
    val connectionTimeouts = config.getLong(HazelCastSinkConfigConstants.CONNECTION_TIMEOUT)
    val keepAlive = config.getBoolean(HazelCastSinkConfigConstants.KEEP_ALIVE)
    val tcpNoDelay = config.getBoolean(HazelCastSinkConfigConstants.TCP_NO_DELAY)
    val reuse = config.getBoolean(HazelCastSinkConfigConstants.REUSE_ADDRESS)
    val linger = config.getInt(HazelCastSinkConfigConstants.LINGER_SECONDS)
    val buffer = config.getInt(HazelCastSinkConfigConstants.BUFFER_SIZE)
    val socketConfig = HazelCastSocketConfig(keepAlive, tcpNoDelay, reuse, linger, buffer)
    val pass = config.getPassword(HazelCastSinkConfigConstants.SINK_GROUP_PASSWORD).value()
    val group = config.getString(HazelCastSinkConfigConstants.SINK_GROUP_NAME)
    new HazelCastConnectionConfig(group, members, redo, connectionAttempts, connectionTimeouts, pass, socketConfig)
  }
}
