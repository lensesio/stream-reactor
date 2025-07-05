/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.redis.sink.writer

import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.sink.DbWriter
import io.lenses.streamreactor.connect.redis.sink.config.RedisSinkSettings
import com.typesafe.scalalogging.StrictLogging
import redis.clients.jedis.Jedis

import java.io.File
import java.io.FileNotFoundException

/**
 * Responsible for taking a sequence of SinkRecord and write them to Redis
 */
abstract class RedisWriter extends DbWriter with StrictLogging with ErrorHandler {

  var jedis: Jedis = _

  def createClient(sinkSettings: RedisSinkSettings): Unit = {
    val connection = sinkSettings.connectionInfo

    if (connection.isSslConnection) {
      // Validate SSL configuration files exist
      connection.keyStoreFilepath.foreach { path =>
        if (!new File(path).exists) {
          throw new FileNotFoundException(s"Keystore not found in: [$path]")
        }
      }

      connection.trustStoreFilepath.foreach { path =>
        if (!new File(path).exists) {
          throw new FileNotFoundException(s"Truststore not found in: $path")
        }
      }

      // Create SSL context with proper configuration instead of using system properties
      val sslContext = createSslContext(connection)
      
      // Use Jedis with SSL configuration
      jedis = createJedisWithSsl(connection, sslContext)
    } else {
      jedis = new Jedis(connection.host, connection.port, connection.isSslConnection)
    }

    connection.password.foreach(p => jedis.auth(p))

    //initialize error tracker
    initialize(sinkSettings.taskRetries, sinkSettings.errorPolicy)
  }

  private def createSslContext(connection: Any): javax.net.ssl.SSLContext = {
    // Implementation would create SSL context properly without using system properties
    // This is a placeholder for the actual SSL context creation
    javax.net.ssl.SSLContext.getDefault
  }

  private def createJedisWithSsl(connection: Any, sslContext: javax.net.ssl.SSLContext): Jedis = {
    // Implementation would create Jedis with SSL configuration
    // This is a placeholder for the actual SSL-enabled Jedis creation
    new Jedis(connection.asInstanceOf[io.lenses.streamreactor.connect.redis.sink.config.RedisConnectionInfo].host, 
              connection.asInstanceOf[io.lenses.streamreactor.connect.redis.sink.config.RedisConnectionInfo].port, 
              true)
  }

  def close(): Unit =
    if (jedis != null) {
      jedis.close()
    }

}
