/**
  * Copyright 2015 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.cassandra.sink

import com.datamountaineer.streamreactor.connect.cassandra.CassandraConnection
import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraSettings}
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkTaskContext

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

//Factory to build
object CassandraWriter {
  def apply(connectorConfig: AbstractConfig, context: SinkTaskContext) : CassandraJsonWriter = {

    val conn = Try(CassandraConnection(connectorConfig)) match {
      case Success(s) => s
      case Failure(f) => throw new ConnectException(s"Couldn't connect to Cassandra.", f)
    }

    val assignedTopics = context.assignment().asScala.map(c=>c.topic()).toList
    val settings = CassandraSettings.configureSink(connectorConfig, assignedTopics)

    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(connectorConfig.getString(CassandraConfigConstants.ERROR_RETRY_INTERVAL).toLong)
    }

    new CassandraJsonWriter(cassCon = conn, settings = settings)
  }
}
