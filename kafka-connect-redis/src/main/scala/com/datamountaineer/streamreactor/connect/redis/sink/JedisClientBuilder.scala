/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.connect.redis.sink

import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkSettings
import redis.clients.jedis.Jedis

import java.io.File
import java.io.FileNotFoundException

object JedisClientBuilder {
  def createClient(sinkSettings: RedisSinkSettings): Jedis = {
    val connection = sinkSettings.connectionInfo

    if (connection.isSslConnection) {
      connection.keyStoreFilepath match {
        case Some(path) =>
          if (!new File(path).exists) {
            throw new FileNotFoundException(s"Keystore not found in: [$path]")
          }

          System.setProperty("javax.net.ssl.keyStorePassword", connection.keyStorePassword.getOrElse(""))
          System.setProperty("javax.net.ssl.keyStore", path)
          System.setProperty("javax.net.ssl.keyStoreType", connection.keyStoreType.getOrElse("jceks"))

        case None =>
      }

      connection.trustStoreFilepath match {
        case Some(path) =>
          if (!new File(path).exists) {
            throw new FileNotFoundException(s"Truststore not found in: $path")
          }

          System.setProperty("javax.net.ssl.trustStorePassword", connection.trustStorePassword.getOrElse(""))
          System.setProperty("javax.net.ssl.trustStore", path)
          System.setProperty("javax.net.ssl.trustStoreType", connection.trustStoreType.getOrElse("jceks"))

        case None =>
      }
    }

    val jedis = new Jedis(connection.host, connection.port, connection.isSslConnection)
    connection.password.foreach(p => jedis.auth(p))
    jedis
  }
}
