package com.datamountaineer.streamreactor.connect.elastic

import com.datamountaineer.streamreactor.connect.{ConnectUtils, Logging}
import com.sksamuel.elastic4s.mappings.FieldType.{IntegerType, GeoPointType}
import com.sksamuel.elastic4s.{IndexDefinition, ElasticClient}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.source.Indexable
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.elasticsearch.action.bulk.BulkResponse
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ElasticJsonWriter(client: ElasticClient, context: SinkTaskContext) extends Logging {

  log.info("Initialising Elastic Json writer")
  private val utils = new ConnectUtils
  val topics = context.assignment().asScala.map(c=>c.topic()).toList
  createIndexes(topics)

  implicit object SinkRecordIndexable extends Indexable[SinkRecord] {
    override def json(t: SinkRecord): String = utils.convertValueToJson(t)
  }

  /**
    * Create indexes for the topics
    *
    * @param topics A list of topics to create indexes for
    * */
  def createIndexes(topics: List[String]) = {
    topics.foreach( t => client.execute( { create index t }))
  }

  /**
    * Close elastic4s client
    * */
  def close() = {
    client.close()
  }

  /**
    * Write SinkRecords to Elastic Search if list is not empty
    *
    * @param records A list of SinkRecords
    * */
  def write(records: List[SinkRecord]) = {
    if (records.isEmpty) log.info("No records received.") else insert(records)
  }

  /**
    * Create a bulk index statement and execute against elastic4s client
    *
    * @param records A list of SinkRecords
    * */
  def insert(records: List[SinkRecord]) : Future[BulkResponse] = {
    val indexes: List[IndexDefinition] = records.map(r => index into r.topic() / r.topic() source r)
    val ret = client.execute(bulk(indexes).refresh(true))

    ret.onSuccess({
      case s => log.info(s"Elastic write successful for ${records.size} records!")
    })

    ret.onFailure( {
      case f:Throwable => log.info(f.toString)
    })
    ret
  }
}