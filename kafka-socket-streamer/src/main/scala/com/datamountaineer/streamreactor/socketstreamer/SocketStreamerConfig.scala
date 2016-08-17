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

package com.datamountaineer.streamreactor.socketstreamer

import java.net.URL

import com.typesafe.config.ConfigFactory

import scala.util.Try


case class SocketStreamerConfig(actorSystemName: String,
                                zookeeper: String,
                                kafkaBrokers: String,
                                schemaRegistryUrl: String,
                                port: Int)

object SocketStreamerConfig {
  def apply() = {
    val config = ConfigFactory.load()
    val systemName = config.getString("system-name")
    require(systemName.trim.length > 0, "Invalid actor system name")

    val zookeepers = config.getString("kafka.zookeeper")
    require(zookeepers.trim.length > 0, "Invalid zookeeper")
    val kafkaBootstrapServers = config.getString("kafka.brokers")

    val schemaRegistryUrl = config.getString("kafka.schema-registry-url")
    require(schemaRegistryUrl.trim.length > 0 && Try(new URL(schemaRegistryUrl)).isSuccess, "Invalid Schema Registry URL")

    val port = config.getInt("port")
    new SocketStreamerConfig(systemName, zookeepers, kafkaBootstrapServers, schemaRegistryUrl, port)
  }
}
