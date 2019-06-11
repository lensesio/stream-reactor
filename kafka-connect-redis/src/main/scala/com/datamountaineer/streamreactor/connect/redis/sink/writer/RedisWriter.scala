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

package com.datamountaineer.streamreactor.connect.redis.sink.writer

import java.io.{File, FileNotFoundException}
import java.net.URI

import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisConfigConstants, RedisSinkSettings}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.datamountaineer.streamreactor.connect.sink._
import com.typesafe.scalalogging.slf4j.StrictLogging
import redis.clients.jedis.Jedis

/**
  * Responsible for taking a sequence of SinkRecord and write them to Redis
  */
abstract class RedisWriter extends DbWriter with StrictLogging with ConverterUtil with ErrorHandler {

  var jedis: Jedis = _

  def apply(sinkSettings: RedisSinkSettings): Unit = {
    val connection = sinkSettings.connectionInfo

    if (connection.isSslConnection) {

        val keyStoreFilepath = connection.keyStoreFilepath.getOrElse("")

        if (!new File(keyStoreFilepath).exists) {
          throw new FileNotFoundException(s"Keystore Certificate not found in: $keyStoreFilepath")
        }

        val trustStoreFilepath = connection.trustStoreFilepath.getOrElse("")

        if (!new File(trustStoreFilepath).exists) {
          throw new FileNotFoundException(s"Truststore Certificate not found in: $trustStoreFilepath")
        }

        System.setProperty("javax.net.ssl.keyStorePassword", connection.keyStorePassword.getOrElse(""))
        System.setProperty("javax.net.ssl.keyStore", keyStoreFilepath)
        System.setProperty("javax.net.ssl.keyStoreType", connection.keyStoreType.getOrElse("jceks"))

        System.setProperty("javax.net.ssl.trustStorePassword", connection.trustStorePassword.getOrElse(""))
        System.setProperty("javax.net.ssl.trustStore", trustStoreFilepath)
        System.setProperty("javax.net.ssl.trustStoreType", connection.trustStoreType.getOrElse("jceks"))

        jedis = new Jedis(URI.create(s"rediss://${connection.host}:${connection.port}"))
    } else {
      jedis = new Jedis(connection.host, connection.port)
    }

    connection.password.foreach(p => jedis.auth(p))

    //initialize error tracker
    initialize(sinkSettings.taskRetries, sinkSettings.errorPolicy)
  }

  def close(): Unit = {
    if (jedis != null) {
      jedis.close()
    }
  }

}

