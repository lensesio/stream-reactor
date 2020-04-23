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

package com.datamountaineer.streamreactor.connect.rethink

import java.io.{BufferedInputStream, FileInputStream}

import com.datamountaineer.streamreactor.connect.rethink.config.ReThinkConfigConstants
import com.rethinkdb.RethinkDB
import com.rethinkdb.net.Connection
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.connect.errors.ConnectException

/**
  * Created by andrew@datamountaineer.com on 27/09/16. 
  * stream-reactor
  */
object ReThinkConnection extends StrictLogging {
  def apply(r: RethinkDB, config: AbstractConfig): Connection = {

    val host = config.getString(ReThinkConfigConstants.RETHINK_HOST)
    val port = config.getInt(ReThinkConfigConstants.RETHINK_PORT)
    val username = config.getString(ReThinkConfigConstants.USERNAME)
    val password = config.getPassword(ReThinkConfigConstants.PASSWORD).value()
    val certFile = config.getString(ReThinkConfigConstants.CERT_FILE)
    val authKey = config.getPassword(ReThinkConfigConstants.AUTH_KEY)

    //java driver also catches this
    if (username.nonEmpty && certFile.nonEmpty) {
      throw new ConnectException("Username and Certificate file can not be used together.")
    }

    if ((certFile.nonEmpty && config.getPassword(ReThinkConfigConstants.AUTH_KEY).value().isEmpty)
      || certFile.isEmpty && config.getPassword(ReThinkConfigConstants.AUTH_KEY).value().nonEmpty
    ) {
      throw new ConnectException("Both the certificate file and authentication key must be set for secure TLS connections.")
    }

    val builder = r.connection()
      .hostname(host)
      .port(port)

    if (!username.isEmpty) {
      logger.info("Adding username/password credentials to connection")
      builder.user(username, password)
    }

    if (!certFile.isEmpty) {
      logger.info(s"Using certificate file ${certFile} for TLS connection, overriding any SSLContext")
      val is = new BufferedInputStream(new FileInputStream(certFile))
      builder.certFile(is)
    }

    if (!authKey.value().isEmpty) {
      logger.info("Set authorization key")
      builder.authKey(authKey.value())
    }

    builder.connect()
  }
}
