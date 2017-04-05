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

package com.datamountaineer.streamreactor.connect.voltdb.writers

import java.util.concurrent.Executors

import com.datamountaineer.streamreactor.connect.concurrent.ExecutorExtension._
import com.datamountaineer.streamreactor.connect.concurrent.FutureAwaitWithFailFastFn
import com.datamountaineer.streamreactor.connect.voltdb.config.VoltSettings
import org.voltdb.client.Client

object VoltConnectionConnectFn extends Retries {
  def apply(client: Client, settings: VoltSettings): Seq[Unit] = {
    logger.info("Connecting to VoltDB...")
    val servers = settings.servers.split(",").map(_.trim)

    val executor = Executors.newFixedThreadPool(servers.length)

    val futures = servers.map { server =>
      executor.submit {
        connectWithRetries(client, server, 10)
      }
    }
    FutureAwaitWithFailFastFn(executor, futures)
  }

  private def connectWithRetries(client: Client, server: String, maxRetries: Int) = {
    val retryInterval = 1000
    withRetries(maxRetries, retryInterval, Some(s"Connection failure. Retrying in $retryInterval")) {
      client.createConnection(server)
    }
    logger.info(s"Connected to VoltDB node at: $server")
  }

}
