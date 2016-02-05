package com.datamountaineer.streamreactor.connect

import java.util.concurrent.CountDownLatch

import com.datastax.driver.core.{ResultSet, PreparedStatement}


import org.apache.kafka.connect.json.{JsonDeserializer, JsonConverter}
import org.apache.kafka.connect.sink.{SinkTaskContext, SinkRecord}

import com.datamountaineer.streamreactor.connect.CassandraWrapper.resultSetFutureToScala
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try


class CassandraJsonWriter(connector: CassandraConnection, context : SinkTaskContext) {
  private val converter = new JsonConverter()
  //initialize the converter cache
  converter.configure(new HashMap[String, String].asJava, false)
  private val deserializer = new JsonDeserializer()
  //get topic list from context assignment
  private val topics = context.assignment().asScala.map(c=>c.topic()).toList
  //cache for prepared statements
  private val preparedCache: Map[String, PreparedStatement] = cachePreparedStatements(topics)

  /**
    * Cache the preparedStatements per topic rather than create them every time
    * Each one is an insert statement aligned to topics
    *
    * @param topics A list of topics
    * @return A Map of topic->preparedStatements
    * */
  private def cachePreparedStatements(topics : List[String]) : Map[String, PreparedStatement] = {
    topics.map(t=> {
      val prepare = getPreparedStatement(t)
      (t, prepare)
    }).toMap
  }

  /**
    * Build a preparedStatement for the given topic
    *
    * @param topic The topic/table name to prepare the statement for
    * @return A prepared statement for the given topic
    * */
  private def getPreparedStatement(topic : String) : PreparedStatement = {
    connector.session.prepare(s"INSERT INTO ${connector.session.getLoggedKeyspace}.${topic} JSON ?")
  }

  /**
    * Convert a SinkRecords value to a Json string using Kafka Connects deserializer
    *
    * @param record A SinkRecord to extract the payload value from
    * @return A json string for the payload of the record
    * */
  private def convertToJson(record: SinkRecord) : String = {
    val converted: Array[Byte] = converter.fromConnectData(record.topic(), record.valueSchema(), record.value())
    deserializer.deserialize(record.topic(), converted).get("payload").toString
  }

  /**
    * Write SinkRecords to Cassandra (aSync) in Json
    *
    * @param records A list of SinkRecords from Kafka Connect to write
    * */
  def write(records : List[SinkRecord]) = {
    val latch : CountDownLatch = new CountDownLatch(records.size)
    records.map( r => {
      val preparedStatement = Try(preparedCache.get(r.topic()).get).getOrElse(getPreparedStatement(r.topic()))
      val results = resultSetFutureToScala(connector.session.executeAsync(preparedStatement.bind(convertToJson(r))))

      results.onSuccess({
        case r:ResultSet => {
          println("Insert successful")
          latch.countDown()
        }
        case _ => {
          println("Error")
        }
      })

      results.onFailure({
        case t:Throwable => {
          println("Insert failed")
          println(t.getMessage)
          latch.countDown()
        }
      })
      results
    })

    latch.await()
  }

  def close(): Unit = {
    connector.session.close()
    connector.cluster.close()
  }
}

//Factory to build
object CassandraWriter {
  def apply(connectorConfig: CassandraSinkConfig, context: SinkTaskContext) = {
    val contactPoints: String = connectorConfig.getString(CassandraSinkConfig.CONTACT_POINTS)
    val keySpace = connectorConfig.getString(CassandraSinkConfig.KEY_SPACE)
    val connector = CassandraConnection(contactPoints = contactPoints, keySpace = keySpace)
    new CassandraJsonWriter(connector = connector, context = context)
  }
}
