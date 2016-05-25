/**
  * Copyright 2016 Datamountaineer.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  **/

package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.streamreactor.connect.KuduConverter
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
  def apply(config: KuduSinkConfig, context: SinkTaskContext)  : KuduWriter = {
    val kuduMaster = config.getString(KuduSinkConfig.KUDU_MASTER)
    logger.info(s"Connecting to Kudu Master at $kuduMaster")
    lazy val client = new KuduClient.KuduClientBuilder(kuduMaster).build()
    new KuduWriter(client = client, context = context)
  }
}

class KuduWriter(client: KuduClient, context: SinkTaskContext) extends StrictLogging with KuduConverter {
  logger.info("Initialising Kudu writer")
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
    val missing = topics
                      .filter(t=> !client.tableExists(t))
                      .map(f=>{
                        logger.error("Missing kudu table for topic $f")
                        f
                      })
    if (!missing.isEmpty) throw new ConnectException(s"No tables found in Kudu for topics ${missing.mkString(",")}")
    topics.map(t =>(t,client.openTable(t))).toMap
  }

  /**
    * Write SinkRecords to Kudu
    *
    * @param records A list of SinkRecords to write
    * */
  def write(records: List[SinkRecord]) : Unit = {
    //group the records by topic to get a map [string, list[sinkrecords]]
    val grouped = records.groupBy(_.topic())
    //for each group get a new insert, convert and apply
    grouped.foreach {
      case (topic, entries) => applyInsert(topic, entries, session)
    }
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
    logger.info(s"Written ${records.size} for $topic")
  }

  /**
    * Close the Kudu session and client
    * */
  def close() : Unit = {
    logger.info("Closing Kudu Session and Client")
    flush()
    if (!session.isClosed) session.close()
    client.shutdown()
  }

  /**
    * Force the session to flush it's buffers.
    *
    * */
  def flush() : Unit = {
    session.flush()
  }
}
