/*
 * Copyright 2017 Datamountaineer.
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
 */

package com.datamountaineer.streamreactor.connect.cassandra.source

import java.util.concurrent.LinkedBlockingQueue

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.{ CassandraConfigSource, CassandraSettings }
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ BeforeAndAfter, Matchers, WordSpec }

import scala.collection.JavaConverters._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.Config
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigConstants
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraSourceSetting

/**
 *
 */
class TestCqlGenerator extends WordSpec with Matchers with BeforeAndAfter with MockitoSugar with TestConfig
    with ConverterUtil {

  "CqlGenerator should generate timeuuid statement based on config" in {

    val cqlGenerator = new CqlGenerator(configureMe("", "timeuuid"))
    val cqlStatement = cqlGenerator.getCqlStatement

    cqlStatement shouldBe "SELECT timestamp_field,string_field FROM sink_test.cassandra-table WHERE timestamp_field > maxTimeuuid(?) AND timestamp_field <= minTimeuuid(?) ALLOW FILTERING"
  }

  "CqlGenerator should generate timestamp statement based on KCQL" in {

    val cqlGenerator = new CqlGenerator(configureMe("INCREMENTALMODE=timestamp", "timeuuid"))
    val cqlStatement = cqlGenerator.getCqlStatement

    cqlStatement shouldBe "SELECT timestamp_field,string_field FROM sink_test.cassandra-table WHERE timestamp_field > ? AND timestamp_field <= ? ALLOW FILTERING"
  }

  "CqlGenerator should generate token based CQL statement based on KCQL" in {

    val cqlGenerator = new CqlGenerator(configureMe("INCREMENTALMODE=token", "timeuuid"))
    val cqlStatement = cqlGenerator.getCqlStatement

    cqlStatement shouldBe "SELECT timestamp_field,string_field FROM sink_test.cassandra-table WHERE token(timestamp_field) > token(?) LIMIT 200"
  }
  
  "CqlGenerator should generate CQL statement with no offset based on KCQL" in {

    val cqlGenerator = new CqlGenerator(configureMe("INCREMENTALMODE=token", "timeuuid"))
    val cqlStatement = cqlGenerator.getCqlStatementNoOffset

    cqlStatement shouldBe "SELECT timestamp_field,string_field FROM sink_test.cassandra-table LIMIT 200"
  }

  "Exception should be thrown with unknown incremental mode in KCQL" in {

    try {
      val cqlGenerator = new CqlGenerator(configureMe("INCREMENTALMODE=bogus", "timeuuid"))
      cqlGenerator.getCqlStatement
      fail()
    } catch {
      case _: ConfigException => // Expected, so continue
    }
  }

  "Exception should be thrown with unknown incremental mode in config" in {

    try {
      val cqlGenerator = new CqlGenerator(configureMe("", "bogus"))
      cqlGenerator.getCqlStatement
      fail()
    } catch {
      case _: NoSuchElementException => // Expected, so continue
    }
  }

  def configureMe(kcqlIncrementMode: String, configIncrementMode: String): CassandraSourceSetting = {
    val myKcql = s"INSERT INTO kafka-topic SELECT string_field, timestamp_field FROM cassandra-table PK timestamp_field BATCH=200 $kcqlIncrementMode"
    val configMap = {
      Map(
        CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
        CassandraConfigConstants.SOURCE_KCQL_QUERY -> myKcql,
        CassandraConfigConstants.ASSIGNED_TABLES -> s"$TABLE3",
        CassandraConfigConstants.IMPORT_MODE -> CassandraConfigConstants.INCREMENTAL,
        CassandraConfigConstants.POLL_INTERVAL -> "1000",
        CassandraConfigConstants.FETCH_SIZE -> "500",
        CassandraConfigConstants.BATCH_SIZE -> "800",
        CassandraConfigConstants.TIMESTAMP_TYPE -> configIncrementMode).asJava
    }
    val configSource = new CassandraConfigSource(configMap)
    CassandraSettings.configureSource(configSource).head
  }

}