package com.datamountaineer.streamreactor.connect

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import com.datastax.driver.core.{ ResultSet, PreparedStatement}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{ObjectMapper, JsonNode}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.json.{JsonDeserializer, JsonConverter}
import org.apache.kafka.connect.sink.{SinkTaskContext, SinkRecord}
import com.datamountaineer.streamreactor.connect.CassandraWrapper.resultSetFutureToScala
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.concurrent.ExecutionContext.Implicits.global
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

/**
  * <h1>CassandraJsonWriter</h1>
  * Cassandra Json writer for Kafka connect
  * Writes a list of Kafka connect sink records to Cassandra using the JSON support
  *
  */
class CassandraJsonWriter(connector: CassandraConnection, context : SinkTaskContext) extends Logging {
  log.info("Initialising Cassandra writer")
  private val converter = new JsonConverter()
  //initialize the converter cache
  converter.configure(new HashMap[String, String].asJava, false)
  private val deserializer = new JsonDeserializer()
  //get topic list from context assignment
  private val topics = context.assignment().asScala.map(c=>c.topic()).toList

  //check a table exists in Cassandra for the topics
  checkCassandraTables(topics, connector.session.getLoggedKeyspace)

  //cache for prepared statements
  private var preparedCache: Map[String, PreparedStatement] = cachePreparedStatements(topics)

  /**
    * Check if we have tables in Cassandra and if we have table named the same as our topic
    *
    * @param topics A list of the assigned topics
    * @param keySpace The keyspace to look in for the tables
    * */
  private def checkCassandraTables(topics: List[String], keySpace: String) = {
    val tables = connector.cluster.getMetadata.getKeyspace(keySpace).getTables.asScala.map(t => t.getName).asJavaCollection
    //check tables
    tables.isEmpty match {
      case true => throw new ConnectException(s"No tables found in Cassandra for keyspace $keySpace")
      case false => //Check we have our topics as tables

        tables.containsAll(topics.asJavaCollection) match {
          case true =>
          case false =>
            val missing = topics.filter(p => !tables.contains(p))
            throw new ConnectException(s"Not table found in Cassandra for topics ${missing.mkString(",")}")
        }
    }
  }

  /**
    * Cache the preparedStatements per topic rather than create them every time
    * Each one is an insert statement aligned to topics
    *
    * @param topics A list of topics
    * @return A Map of topic->preparedStatements
    * */
  private def cachePreparedStatements(topics : List[String]) : Map[String, PreparedStatement] = {
    log.info(s"Preparing statements for ${topics.mkString(",")}.")
    topics.distinct.map( t=> {
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
    connector.session.prepare(s"INSERT INTO ${connector.session.getLoggedKeyspace}.$topic JSON ?")
  }

  /**
    * Convert a SinkRecords value to a Json string using Kafka Connects deserializer
    *
    * @param record A SinkRecord to extract the payload value from
    * @return A json string for the payload of the record
    * */
  private def convertValueToJson(record: SinkRecord) : String = {
    val converted: Array[Byte] = converter.fromConnectData(record.topic(), record.valueSchema(), record.value())
    val json = deserializer.deserialize(record.topic(), converted).get("payload").toString
    log.debug(s"Converted payload to $json.")
    json
  }

  /**
    * Convert a SinkRecords key to a Json string using Kafka Connects deserializer
    *
    * @param record A SinkRecord to extract the payload value from
    * @return A json string for the payload of the record
    * */
  private def convertKeyToJson(record: SinkRecord) : String = {
    val converted: Array[Byte] = converter.fromConnectData(record.topic(), record.keySchema(), record.key())
    val json = deserializer.deserialize(record.topic(), converted).get("key").toString
    log.debug(s"Converted key to $json.")
    json
  }

  /**
    * Write SinkRecords to Cassandra (aSync) in Json
    *
    * @param records A list of SinkRecords from Kafka Connect to write
    * @return boolean indication successful write
    * */
  def write(records : List[SinkRecord]) = {
    val count = records.size

    count match {
      case 0 => log.info("No records received.")
      case _ => {
        val latch : CountDownLatch = new CountDownLatch(count)
        val status : AtomicBoolean = new AtomicBoolean(true)
        records.map( r => {
          //get the preparedStatement, else create and add to cache
          val preparedStatement = preparedCache.contains(r.topic()) match {
            case true => preparedCache.get(r.topic()).get
            case false =>
              val preparedStatement : PreparedStatement = getPreparedStatement(r.topic())
              preparedCache += (r.topic() -> preparedStatement)
              preparedStatement
          }

          //execute async and convert FutureResultSet to Scala future
          val results = resultSetFutureToScala(connector.session.executeAsync(preparedStatement.bind(convertValueToJson(r))))

          //increment latch
          results.onSuccess({
            case r:ResultSet =>
              log.debug("Insert successful!")
              latch.countDown()
          })

          //increment latch but set status to false
          results.onFailure({
            case t:Throwable =>
              log.warn(s"Insert failed for ! ${t.getMessage}")
              status.set(false)
              latch.countDown()
          })
          results
        })
        log.info("Waiting for write to complete.")
        latch.await()
        status.get match {
          case true => log.info(s"Write complete for $count records.")
          case false => log.warn(s"Failed to write all records!")
        }
        status.get
      }
    }
  }

  /**
    * Closed down the driver session and cluster
    * */
  def close(): Unit = {
    log.info("Shutting down Cassandra driver session and cluster")
    connector.session.close()
    connector.cluster.close()
  }
}