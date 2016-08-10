/**
  * Copyright 2016 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.hazelcast

import com.datamountaineer.streamreactor.connect.hazelcast.config.{HazelCastConnectionConfig, HazelCastSocketConfig}
import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.{ClientConfig, ClientNetworkConfig, SocketOptions}
import com.hazelcast.config.GroupConfig
import com.hazelcast.core.HazelcastInstance
/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */
object HazelCastConnection {
 def apply(config: HazelCastConnectionConfig): HazelcastInstance = {
   val clientConfig = new ClientConfig
   val networkConfig = clientConfig.getNetworkConfig
   val groupConfig = new GroupConfig(config.group, config.pass)
   clientConfig.setGroupConfig(groupConfig)
   buildSocketOptions(networkConfig, config.socketConfig)
   clientConfig.setInstanceName(config.group + "-kafka-connect")
   //val credentials = new UsernamePasswordCredentials("", "")
   HazelcastClient.newHazelcastClient(clientConfig)
 }

 private def buildSocketOptions(clientNetworkConfig: ClientNetworkConfig, socketConfig: HazelCastSocketConfig): SocketOptions = {
   val socketOptions = clientNetworkConfig.getSocketOptions
   socketOptions.setKeepAlive(socketConfig.keepAlive)
   socketOptions.setTcpNoDelay(socketConfig.tcpNoDelay)
   socketOptions.setReuseAddress(socketConfig.reuseAddress)
   socketOptions.setLingerSeconds(socketConfig.lingerSeconds)
   socketOptions.setBufferSize(socketConfig.bufferSize)
   socketOptions
 }
}
