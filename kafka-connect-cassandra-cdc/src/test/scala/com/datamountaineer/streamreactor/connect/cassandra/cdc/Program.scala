package com.datamountaineer.streamreactor.connect.cassandra.cdc

import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.{CassandraConfig, CdcConfig, CdcSubscription}
import com.datamountaineer.streamreactor.connect.cassandra.cdc.logs.CdcCassandra
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.collection.JavaConversions._

object Program extends App with StrictLogging {
  val connection = CassandraConfig(
    "127.0.0.1",
    9042,
    "cassandra.yaml",
    None,
    None,
    None
  )
  implicit val config = CdcConfig(connection,
    Seq(
      CdcSubscription("datamountaineer", "orders", "orders")
    ))
  val cdcCassandra = new CdcCassandra()

  var stop = false
  cdcCassandra.start(None)
  while (!stop) {
    Thread.sleep(4000)
    val changes = cdcCassandra.getMutations()
    if (changes.nonEmpty) {
      changes.foreach { change =>
        logger.info(s"Change:$change")
      }
    }
  }
}
