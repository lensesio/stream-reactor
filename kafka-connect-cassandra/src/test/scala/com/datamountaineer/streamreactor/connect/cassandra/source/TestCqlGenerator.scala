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

import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraConfigSource, CassandraSettings, CassandraSourceSetting}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

/**
 *
 */
class TestCqlGenerator extends AnyWordSpec
    with Matchers 
    with MockitoSugar 
    with ConverterUtil {

  
  "CqlGenerator should generate timeuuid statement based on KCQL" in {

    val cqlGenerator = new CqlGenerator(configureMe("INCREMENTALMODE=timeuuid"))
    val cqlStatement = cqlGenerator.getCqlStatement

    cqlStatement shouldBe "SELECT string_field,the_pk_field FROM test.cassandra-table WHERE the_pk_field > maxTimeuuid(?) AND the_pk_field <= minTimeuuid(?) ALLOW FILTERING"
  }

  "CqlGenerator should generate timestamp statement based on KCQL" in {

    val cqlGenerator = new CqlGenerator(configureMe("INCREMENTALMODE=timestamp"))
    val cqlStatement = cqlGenerator.getCqlStatement

    cqlStatement shouldBe "SELECT string_field,the_pk_field FROM test.cassandra-table WHERE the_pk_field > ? AND the_pk_field <= ? ALLOW FILTERING"
  }

  "CqlGenerator should generate dse search timestamp statement based on KCQL" in {

    val cqlGenerator = new CqlGenerator(configureMe("INCREMENTALMODE=dsesearchtimestamp"))
    val cqlStatement = cqlGenerator.getCqlStatement

    cqlStatement shouldBe "SELECT string_field,the_pk_field FROM test.cassandra-table WHERE solr_query=?"
  }

  "CqlGenerator should generate token based CQL statement based on KCQL" in {

    val cqlGenerator = new CqlGenerator(configureMe("INCREMENTALMODE=token"))
    val cqlStatement = cqlGenerator.getCqlStatement

    cqlStatement shouldBe "SELECT string_field,the_pk_field FROM test.cassandra-table WHERE token(the_pk_field) > token(?) LIMIT 200"
  }
  
  "CqlGenerator should generate CQL statement with no offset based on KCQL" in {

    val cqlGenerator = new CqlGenerator(configureMe("INCREMENTALMODE=token"))
    val cqlStatement = cqlGenerator.getCqlStatementNoOffset

    cqlStatement shouldBe "SELECT string_field,the_pk_field FROM test.cassandra-table LIMIT 200"
  }
  
  "CqlGenerator should generate format type json CQL statement based on KCQL" in {

    val cqlGenerator = new CqlGenerator(configureJSON())
    val cqlStatement = cqlGenerator.getCqlStatement

    cqlStatement shouldBe "SELECT string_field,the_pk_field FROM test.cassandra-table WHERE the_pk_field > ? AND the_pk_field <= ? ALLOW FILTERING"
  }
  
  "CqlGenerator should generate format type json CQL with keys statement based on KCQL" in {

    val cqlGenerator = new CqlGenerator(configureJSONWithKey("string_field"))
    val cqlStatement = cqlGenerator.getCqlStatement

    cqlStatement shouldBe "SELECT string_field,the_pk_field FROM test.cassandra-table WHERE the_pk_field > ? AND the_pk_field <= ? ALLOW FILTERING"
  }
  
  def configureJSON(): CassandraSourceSetting = {
    val myKcql = s"INSERT INTO kafka-topic SELECT string_field, the_pk_field FROM cassandra-table PK the_pk_field WITHFORMAT JSON WITHUNWRAP INCREMENTALMODE=TIMESTAMP"
    val configMap = {
      Map(
        CassandraConfigConstants.KEY_SPACE -> "test",
        CassandraConfigConstants.KCQL -> myKcql,
        CassandraConfigConstants.ASSIGNED_TABLES -> "cassandra-table",
        CassandraConfigConstants.POLL_INTERVAL -> "1000",
        CassandraConfigConstants.FETCH_SIZE -> "500",
        CassandraConfigConstants.BATCH_SIZE -> "800").asJava
    }
    val configSource = new CassandraConfigSource(configMap)
    CassandraSettings.configureSource(configSource).head
  }
  
  def configureJSONWithKey(keyField: String): CassandraSourceSetting = {
    val myKcql = s"INSERT INTO kafka-topic SELECT string_field, the_pk_field FROM cassandra-table PK the_pk_field WITHFORMAT JSON WITHUNWRAP INCREMENTALMODE=TIMESTAMP WITHKEY($keyField)"
    val configMap = {
      Map(
        CassandraConfigConstants.KEY_SPACE -> "test",
        CassandraConfigConstants.KCQL -> myKcql,
        CassandraConfigConstants.ASSIGNED_TABLES -> "cassandra-table",
        CassandraConfigConstants.POLL_INTERVAL -> "1000",
        CassandraConfigConstants.FETCH_SIZE -> "500",
        CassandraConfigConstants.BATCH_SIZE -> "800").asJava
    }
    val configSource = new CassandraConfigSource(configMap)
    CassandraSettings.configureSource(configSource).head
  }


  def configureMe(kcqlIncrementMode: String): CassandraSourceSetting = {
    val myKcql = s"INSERT INTO kafka-topic SELECT string_field, the_pk_field FROM cassandra-table PK the_pk_field BATCH=200 $kcqlIncrementMode"
    val configMap = {
      Map(
        CassandraConfigConstants.KEY_SPACE -> "test",
        CassandraConfigConstants.KCQL -> myKcql,
        CassandraConfigConstants.ASSIGNED_TABLES -> "cassandra-table",
        CassandraConfigConstants.POLL_INTERVAL -> "1000",
        CassandraConfigConstants.FETCH_SIZE -> "500",
        CassandraConfigConstants.BATCH_SIZE -> "800").asJava
    }
    val configSource = new CassandraConfigSource(configMap)
    CassandraSettings.configureSource(configSource).head
  }

}