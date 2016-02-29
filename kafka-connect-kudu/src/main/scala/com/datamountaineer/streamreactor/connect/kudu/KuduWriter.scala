package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.streamreactor.connect.KuduConverter
import com.datamountaineer.streamreactor.connect.utils.Logging
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.kududb.client._
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */
object KuduWriter extends Logging {
  def apply(config: KuduSinkConfig, context: SinkTaskContext) = {
    val kuduMaster = config.getString(KuduSinkConfig.KUDU_MASTER)
    log.info(s"Connecting to Kudu Master at $kuduMaster")
    val client = new KuduClient.KuduClientBuilder(kuduMaster).build()
    new KuduWriter(client = client, context = context)
  }
}

class KuduWriter(client: KuduClient, context: SinkTaskContext) extends Logging with KuduConverter {
  log.info("Initialising Kudu writer")
  private val topics = context.assignment().asScala.map(c=>c.topic()).toList
  private val kuduTableInserts = buildTableCache(topics)
  private val session = client.newSession()
  session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND)
  session.isIgnoreAllDuplicateRows

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
    grouped.foreach(g=>
      {
        applyInsert(g._1, g._2, session)
        log.info(s"Written ${records.size} for ${g._1}")
      }
    )
    flush()
  }

  /**
    * Per topic, build an new Kudu insert. Per insert build a Kudu row per SinkRecord.
    * Apply the insert per topic for the rows
    * */
  private def applyInsert(topic: String, records: List[SinkRecord], session: KuduSession) = {
    //get a new insert
    val table = kuduTableInserts.get(topic).get
    log.debug(s"Preparing write for $topic.")
    records
      .map(r=>{
        val insert = table.newInsert()
        convertToKudu(r, insert.getRow)
        insert
      })
      .foreach(i=>session.apply(i))
  }

  /**
    * Convert a SinkRecord to a Kudu Row
    *
    * @param record A SinkRecord to convert
    * @return A Kudu Row
    * */
  private def convertToKudu(record: SinkRecord, row : PartialRow) : PartialRow = {
    val fields =  record.valueSchema().fields().asScala
    fields.foreach(f =>convertTypeAndAdd(fieldType = f.schema().`type`(), fieldName = f.name(), record = record, row = row))
    row
  }

  /**
    * Close the Kudu session and client
    * */
  def close() = {
    log.info("Closing Kudu Session and Client")
    flush()
    if (!session.isClosed) session.close()
    client.shutdown()
  }

  /**
    * Force the session to flush it's buffers.
    *
    * */
  def flush() = {
    session.flush()
  }
}
