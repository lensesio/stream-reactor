package com.datamountaineer.streamreactor.connect.cassandra

import java.util.concurrent.atomic.AtomicLong
import com.datamountaineer.streamreactor.connect.{ConnectUtils, Logging}
import com.datamountaineer.streamreactor.connect.cassandra.CassandraWrapper.resultSetFutureToScala
import com.datastax.driver.core.{PreparedStatement, ResultSet}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * <h1>CassandraJsonWriter</h1>
  * Cassandra Json writer for Kafka connect
  * Writes a list of Kafka connect sink records to Cassandra using the JSON support
  *
  */
class CassandraJsonWriter(connector: CassandraConnection, context : SinkTaskContext) extends Logging {
  log.info("Initialising Cassandra writer")
  private val utils = new ConnectUtils()
  val insertCount = new AtomicLong()

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
    val metaData = connector.cluster.getMetadata.getKeyspace(keySpace).getTables.asScala
    val tables: List[String] = metaData.map(t=>t.getName).toList

    //check tables
    if (tables.isEmpty) throw new ConnectException(s"No tables found in Cassandra for keyspace $keySpace")

    //check we have a table for all topics
    val missing = topics.filter( tp => !tables.contains(tp))

    if (missing.nonEmpty) throw new ConnectException(s"Not table found in Cassandra for topics ${missing.mkString(",")}")
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
    topics.distinct.map( t=> (t, getPreparedStatement(t))).toMap
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
    * Write SinkRecords to Cassandra (aSync) in Json
    *
    * @param records A list of SinkRecords from Kafka Connect to write
    * */
  def write(records : List[SinkRecord]) = {
    if (records.isEmpty) log.info("No records received.") else insert(records)
  }


  /**
    * Write SinkRecords to Cassandra (aSync) in Json
    *
    * @param records A list of SinkRecords from Kafka Connect to write
    * @return boolean indication successful write
    * */
  def insert(records: List[SinkRecord]) = {
    insertCount.addAndGet(records.size)
    records.map( r => {
      //get the preparedStatement, else create and add to cache
      val preparedStatement = preparedCache.contains(r.topic()) match {
        case true => preparedCache.get(r.topic()).get
        case false =>
          val preparedStatement : PreparedStatement = getPreparedStatement(r.topic())
          //add to cache
          preparedCache += (r.topic() -> preparedStatement)
          preparedStatement
      }

      //execute async and convert FutureResultSet to Scala future
      val results = resultSetFutureToScala(connector.session.executeAsync(preparedStatement.bind(utils.convertValueToJson(r))))

      //increment latch
      results.onSuccess({
        case r:ResultSet =>
          log.debug("Insert successful!")
          insertCount.decrementAndGet()
      })

      //increment latch but set status to false
      results.onFailure({
        case t:Throwable =>
          log.warn(s"Insert failed for ! ${t.getMessage}")
          insertCount.decrementAndGet()
      })
      results
    })
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
