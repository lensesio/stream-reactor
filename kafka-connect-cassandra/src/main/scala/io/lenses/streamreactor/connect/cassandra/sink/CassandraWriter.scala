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
package io.lenses.streamreactor.connect.cassandra.sink

import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.connect.cassandra.CassandraConnection
import io.lenses.streamreactor.connect.cassandra.config.CassandraConfigConstants
import io.lenses.streamreactor.connect.cassandra.config.CassandraConfigSink
import io.lenses.streamreactor.connect.cassandra.config.CassandraSettings
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkTaskContext

import scala.util.Failure
import scala.util.Success
import scala.util.Try

//Factory to build
object CassandraWriter extends StrictLogging {
  def apply(connectorConfig: CassandraConfigSink, context: SinkTaskContext): CassandraJsonWriter = {

    val connection = Try(CassandraConnection(connectorConfig)) match {
      case Success(s) => s
      case Failure(f) => throw new ConnectException(s"Couldn't connect to Cassandra.", f)
    }

    val settings = CassandraSettings.configureSink(connectorConfig)
    //if error policy is retry set retry interval
    settings.errorPolicy match {
      case RetryErrorPolicy() =>
        context.timeout(connectorConfig.getInt(CassandraConfigConstants.ERROR_RETRY_INTERVAL).toLong)
      case _ =>
    }

    new CassandraJsonWriter(connection, settings)
  }
}
