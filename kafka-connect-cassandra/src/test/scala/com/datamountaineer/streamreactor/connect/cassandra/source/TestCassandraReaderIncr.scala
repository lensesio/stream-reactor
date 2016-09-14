package com.datamountaineer.streamreactor.connect.cassandra.source

import java.util.concurrent.LinkedBlockingQueue

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigSource, CassandraSettings}
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 04/05/16.
  * stream-reactor
  */
class TestCassandraReaderIncr extends WordSpec with Matchers with BeforeAndAfter with MockitoSugar
  with TestConfig with ConverterUtil {

  before {
    startEmbeddedCassandra()
  }

  "CassandraReader should read a tables records in incremental mode" in {
    val session =  createTableAndKeySpace(secure = true, ssl = false)

    val sql = s"INSERT INTO $CASSANDRA_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 2, 3, 'magic_string', now());"
    session.execute(sql)

    val taskContext = getSourceTaskContextDefault
    val taskConfig  = new CassandraConfigSource(getCassandraConfigSourcePropsIncr)
    val assigned = List(TABLE2)

    //queue for reader to put records in
    val queue = new LinkedBlockingQueue[SourceRecord](100)
    val setting = CassandraSettings.configureSource(taskConfig).head
    val reader = CassandraTableReader(session = session, setting = setting, context = taskContext, queue = queue)
    reader.read()

    //sleep and check queue size
    while (reader.isQuerying) {
      Thread.sleep(1000)
    }
    queue.size() shouldBe 1

    //drain the queue
    val sourceRecords = QueueHelpers.drainQueue(queue, 1).asScala.toList
    val sourceRecord = sourceRecords.head
    //check a field
    val json: JsonNode = convertValueToJson(sourceRecord)
    json.get("string_field").asText().equals("magic_string") shouldBe true

    //insert another two records
    val sql2 = s"INSERT INTO $CASSANDRA_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id2', 3, 4, 'magic_string2', now());"
    session.execute(sql2)
    val sql3 = s"INSERT INTO $CASSANDRA_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id3', 4, 5, 'magic_string3', now());"
    session.execute(sql3)
    Thread.sleep(1000)

    //read
    reader.read()

    //sleep and check queue size
    while (reader.isQuerying) {
      Thread.sleep(1000)
    }

    queue.size() shouldBe 2

    val sourceRecords2 = QueueHelpers.drainQueue(queue, 100).asScala.toList
    sourceRecords2.size shouldBe 2

    //don't insert any new rows
    reader.read()

    //sleep and check queue size
    while (reader.isQuerying) {
      Thread.sleep(1000)
    }

    //should be empty
    queue.size() shouldBe 0

    //insert another two records
    val sql4 = s"INSERT INTO $CASSANDRA_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id4', 4, 5, 'magic_string4', now());"
    session.execute(sql4)

    Thread.sleep(2000)
    val sql5 = s"INSERT INTO $CASSANDRA_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id4', 5, 6, 'magic_string5', now());"
    session.execute(sql5)

    //read!!!!
    reader.read()

    //sleep and check queue size
    while (reader.isQuerying) {
      Thread.sleep(2000)
    }
    //should be 2!!!
    queue.size() shouldBe 2

  }
}
