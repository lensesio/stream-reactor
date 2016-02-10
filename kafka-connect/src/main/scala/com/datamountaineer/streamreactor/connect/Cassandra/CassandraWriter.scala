package com.datamountaineer.streamreactor.connect.cassandra

import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkTaskContext

import scala.util.{Failure, Success, Try}

//Factory to build
object CassandraWriter {
  def apply(connectorConfig: CassandraSinkConfig, context: SinkTaskContext) = {
    val contactPoints: String = connectorConfig.getString(CassandraSinkConfig.CONTACT_POINTS)
    val keySpace = connectorConfig.getString(CassandraSinkConfig.KEY_SPACE)
    val port = connectorConfig.getInt(CassandraSinkConfig.PORT)
    val connector = Try(CassandraConnection(contactPoints = contactPoints,port = port, keySpace = keySpace)) match {
      case Success(s) => s
      case Failure(f) => throw new ConnectException(s"Couldn't connect to Cassandra on $contactPoints:$port", f)
    }
    new CassandraJsonWriter(connector = connector, context = context)
  }
}
