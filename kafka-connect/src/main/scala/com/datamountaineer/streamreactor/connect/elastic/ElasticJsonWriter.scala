package com.datamountaineer.streamreactor.connect.elastic

import com.datamountaineer.streamreactor.connect.{ConnectUtils, Logging}
import com.sksamuel.elastic4s.{BulkDefinition, ElasticClient}
import com.sksamuel.elastic4s.ElasticDsl._
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import scala.collection.JavaConverters._

class ElasticJsonWriter(client: ElasticClient, context: SinkTaskContext) extends Logging {

  //implicit conversion to json for SinkRecord
//  implicit object SinkRecordIndexable extends Indexable[SinkRecord] {
//    override def json(t: SinkRecord): String = utils.convertValueToJson(t)
//  }

  log.info("Initialising Elastic Json writer")
  private val utils = new ConnectUtils
  createIndexes(context.assignment().asScala.map(c=>c.topic()).toList)

  /**
    * Create indexes for the topics
    *
    * @param topics A list of topics to create indexes for
    * */
  def createIndexes(topics: List[String]) = {
    topics.foreach( t => client.execute( { create index t shards 3 replicas 2}))
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
  def insert(records: List[SinkRecord]) = {
    val bulkExecute: List[BulkDefinition] = records.map(r=> bulk(index into r.topic() source utils.convertValueToJson(r)))
    client.execute({bulkExecute})
  }
}