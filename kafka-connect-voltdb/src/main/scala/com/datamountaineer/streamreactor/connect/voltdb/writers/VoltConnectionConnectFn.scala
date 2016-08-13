package com.datamountaineer.streamreactor.connect.voltdb.writers

import java.util.concurrent.Executors

import com.datamountaineer.streamreactor.connect.concurrent.ExecutorExtension._
import com.datamountaineer.streamreactor.connect.concurrent.FutureAwaitWithFailFastFn
import com.datamountaineer.streamreactor.connect.voltdb.config.VoltSettings
import org.voltdb.client.Client

object VoltConnectionConnectFn extends Retries {
  def apply(client: Client, settings: VoltSettings) = {
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
    var retryInterval = 1000
    withRetries(maxRetries, retryInterval, Some(s"Connection failer. Retrying in $retryInterval")) {
      client.createConnection(server)
    }
    logger.info(s"Connected to VoltDB node at: $server")
  }

}
