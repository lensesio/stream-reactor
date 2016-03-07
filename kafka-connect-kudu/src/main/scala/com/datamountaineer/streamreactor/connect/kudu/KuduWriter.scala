package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.streamreactor.connect.KuduConverter
import com.datamountaineer.streamreactor.connect.utils.ConverterUtil
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.kududb.client._

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */
object KuduWriter extends StrictLogging {
  def apply(config: KuduSinkConfig, context: SinkTaskContext) = {
    val kuduMaster = config.getString(KuduSinkConfig.KUDU_MASTER)
    logger.info(s"Connecting to Kudu Master at $kuduMaster")
    lazy val client = new KuduClient.KuduClientBuilder(kuduMaster).build()
    new KuduWriter(client = client, context = context)
  }
}

class KuduWriter(client: KuduClient, context: SinkTaskContext) extends StrictLogging with KuduConverter with ConverterUtil {
  logger.info("Initialising Kudu writer")
  require(context !=null, "Context can not be null!")
  private val topics = context.assignment().asScala.map(c=>c.topic()).toList
  logger.info(s"Assigned topics ${topics.mkString(",")}")
  private lazy val kuduTableInserts = buildTableCache(topics)
  private lazy val session = client.newSession()
  session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND)
  session.isIgnoreAllDuplicateRows

  /**
    * Build a cache of Kudu insert statements per topic and check tables exists for topics
    *
    * @param topics Topic list, we are expecting pre created tables in Kudu
    * @return A Map of topic -> KuduRowInsert
    **/
  private def buildTableCache(topics: List[String]): Map[String, KuduTable] = {
    val missing = topics.filter(t=> !client.tableExists(t)).map(f=>logger.error("Missing kudu table for topic $f"))
    if (missing.isEmpty) throw new ConnectException(s"Not tables found in Kudu for topics ${missing.mkString(",")}")
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
        logger.info(s"Written ${records.size} for ${g._1}")
      }
    )
    flush()
  }

  /**
    * Per topic, build an new Kudu insert. Per insert build a Kudu row per SinkRecord.
    * Apply the insert per topic for the rows
    * */
  private def applyInsert(topic: String, records: List[SinkRecord], session: KuduSession) = {
    val table = kuduTableInserts.get(topic).get
    logger.debug(s"Preparing write for $topic.")
    records
      .map(r=>convert(r, table))
      .foreach(i=>session.apply(i))
  }

  /**
    * Close the Kudu session and client
    * */
  def close() = {
    logger.info("Closing Kudu Session and Client")
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
