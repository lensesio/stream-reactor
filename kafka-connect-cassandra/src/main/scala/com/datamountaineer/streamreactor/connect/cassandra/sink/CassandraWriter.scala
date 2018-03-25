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

package com.datamountaineer.streamreactor.connect.cassandra.sink

import com.datamountaineer.streamreactor.connect.cassandra.CassandraConnection
import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraConfigSink, CassandraSettings}
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkTaskContext

import scala.util.{Failure, Success, Try}

//Factory to build
object CassandraWriter extends StrictLogging {
  def apply(connectorConfig: CassandraConfigSink, context: SinkTaskContext): CassandraJsonWriter = {

    val connection = Try(CassandraConnection(connectorConfig)) match {
      case Success(s) => s
      case Failure(f) => throw new ConnectException(s"Couldn't connect to Cassandra.", f)
    }

    val settings = CassandraSettings.configureSink(connectorConfig)
    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(connectorConfig.getString(CassandraConfigConstants.ERROR_RETRY_INTERVAL).toLong)
    }

    new CassandraJsonWriter(connection, settings)
  }
}
