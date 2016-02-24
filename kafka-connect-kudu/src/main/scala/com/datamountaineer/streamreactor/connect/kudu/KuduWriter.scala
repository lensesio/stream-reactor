package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.streamreactor.connect.{ConnectUtils, Logging}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.kududb.client._
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */
object KuduWriter {
  def apply(config: KuduSinkConfig, context: SinkTaskContext) = {
    val kuduMaster = config.getString(KuduSinkConfig.KUDU_MASTER)
    val client = new KuduClient.KuduClientBuilder(kuduMaster).build()
    new KuduWriter(client = client, context = context)
  }
}

class KuduWriter(client: KuduClient, context: SinkTaskContext) extends Logging {
  log.info("Initialising Kudu writer")
  private val topics = context.assignment().asScala.map(c=>c.topic()).toList
  private val kuduTableInserts = buildTableCache(topics)
  private val session = client.newSession()
  session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND)
  private val utils = new ConnectUtils

  /**
    * Build a cache of Kudu insert statements per topic
    *
    * @param topics Topic list, we are expecting pre created tables in Kudu
    * @return A Map of topic -> KuduRowInsert
    **/
  private def buildTableCache(topics: List[String]): Map[String, KuduTable] = {
    topics.map(t =>(t,client.openTable(t))).toMap
  }

  /**
    * Write SinkRecords to Kudu
    *
    * @param records A list of SinkRecords to write
    * */
  def write(records: List[SinkRecord]) = {
    //group the records by topic to get a map [string, list[sinkrecords]]
    val grouped = records.groupBy(_.topic())
    //for each group get a new insert, convert and apply
    grouped.map(g=>applyInsert(g._1, g._2, session))
  }

  /**
    * Per topic, build an new Kudu insert. Per insert build a Kudu row per SinkRecord.
    * Apply the insert per topic for the rows
    * */
  private def applyInsert(topic: String, records: List[SinkRecord], session: KuduSession) = {
    //get a new insert
    val table = kuduTableInserts.get(topic).get
    records
      .map(r=>convertToKudu(r, table.newInsert()))
      .map(i=>session.apply(i))
  }

  /**
    * Convert a SinkRecord to a Kudu Row
    *
    * @param record A SinkRecord to convert
    * @return A Kudu Row
    * */
  private def convertToKudu(record: SinkRecord, insert : Insert) : Insert = {
    val fields =  record.valueSchema().fields().asScala
    val row = insert.getRow
    fields.foreach(f => utils.convertTypeAndAdd(f.schema().`type`(), f.name(), record, row))
    insert
  }

  /**
    * Close the Kudu session and client
    * */
  def close() = {
    session.close()
    client.close()
  }
}
