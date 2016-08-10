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
