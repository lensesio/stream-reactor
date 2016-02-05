package com.datamountaineer.streamreactor.connect

import java.util

import com.datastax.driver.core.{BoundStatement, Cluster, Session}

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Created by andrew on 22/01/16.
  */
class CassandraSinkTask extends SinkTask {
  var session : Option[Session] = None
  var cluster : Option[Cluster] = None
  var keySpace : String = ""

  override def stop(): Unit = {
    session.get.close()
    cluster.get.close()
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    prepareBoundStatement(records.asScala.toList).map( b => session.get.executeAsync(b))
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}

  override def start(props: util.Map[String, String]): Unit = {
    val cassConfig = new CassandraSinkConfig(props)
    keySpace = cassConfig.getString(CassandraSinkConstants.KEY_SPACE)
    val conUri = CassandraConnectionUri(contactPoints = cassConfig.getString(CassandraSinkConstants.CONTACT_POINTS),
                                        port = cassConfig.getInt(CassandraSinkConstants.PORT),
                                        keySpace = keySpace)

    session = Try(CassandraConnection.createSessionAndInitKeyspace(uri = conUri)) match {
      case Success(s) => Some(s)
      case Failure(f) => throw new Exception(f)
    }

    cluster = Some(session.get.getCluster)
  }

  //fix
  override def version(): String = "unknown"

  def prepareBoundStatement(records : List[SinkRecord] )  : List[BoundStatement] = {
    records.map(r => session.get.prepare(s"INSERT INTO $keySpace.${r.topic()} JSON '?';").bind(r.value()))
  }
}
