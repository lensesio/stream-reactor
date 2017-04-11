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

package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.kafka.EmbeddedSingleNodeKafkaCluster
import com.datamountaineer.kafka.schemaregistry.RestApp
import com.datamountaineer.streamreactor.connect.kudu.config.{KuduSettings, KuduSinkConfig, KuduSinkConfigConstants}
import com.datamountaineer.streamreactor.connect.kudu.sink.{CreateTableProps, DbHandler}
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.kududb.client._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 13/06/16. 
  * stream-reactor-maven
  */
class TestDbHandler extends TestBase with MockitoSugar with KuduConverter {

  "Should identify new columns in schema2" in {
    val diff = DbHandler.compare(createSchema, createSchema2)
    diff.size shouldBe 1
  }

  "Should identify new columns in schema4 with default" in {
    val diff = DbHandler.compare(createSchema, createSchema4)
    diff.size shouldBe 1
  }

  "Should not identify new columns in schema" in {
    val diff = DbHandler.compare(createSchema, createSchema)
    diff.size shouldBe 0
  }

  "Should throw because auto create with no distribute by keys" in {
    val config = new KuduSinkConfig(getConfig)
    val settings = KuduSettings(config)
    val schema =
      """
        |{ "type": "record",
        |"name": "Person",
        |"namespace": "com.datamountaineer",
        |"fields": [
        |{      "name": "name",      "type": "string"},
        |{      "name": "adult",     "type": "boolean"},
        |{      "name": "integer8",  "type": "int"},
        |{      "name": "integer16", "type": "int"},
        |{      "name": "integer32", "type": "long"},
        |{      "name": "integer64", "type": "long"},
        |{      "name": "float32",   "type": "float"},
        |{      "name": "float64",   "type": "double"}
        |]}"
      """.stripMargin

    intercept[ConnectException] {
      settings.routes.map(r => DbHandler.getKuduSchema(r, schema))
    }
  }

  "Should return a Kudu create schema" in {
    val config = new KuduSinkConfig(getConfigAutoCreate(""))
    val settings = KuduSettings(config)

    val creates = settings.routes.map(r => DbHandler.getKuduSchema(r, schema))
    val create = creates.head
    create.getColumnCount shouldBe 8
    create.getPrimaryKeyColumnCount shouldBe 2
    val cols = create.getColumns
    val pks = create.getPrimaryKeyColumns
    pks.get(0).getName shouldBe "name"
    pks.get(0).getType shouldBe org.kududb.Type.STRING

    pks.get(1).getName shouldBe "adult"
    pks.get(1).getType shouldBe org.kududb.Type.BOOL

    //get nullable since no default in avro
    cols.get(2).isNullable shouldBe true
  }

  "Should return a Kudu Create schema with default" in {
    val config = new KuduSinkConfig(getConfigAutoCreate(""))
    val settings = KuduSettings(config)

    val creates = settings.routes.map(r => DbHandler.getKuduSchema(r, schemaDefaults))
    val create = creates.head
    create.getColumnCount shouldBe 8
    create.getPrimaryKeyColumnCount shouldBe 2
    val cols = create.getColumns
    val pks = create.getPrimaryKeyColumns
    pks.get(0).getName shouldBe "name"
    pks.get(0).getType shouldBe org.kududb.Type.STRING

    pks.get(1).getName shouldBe "adult"
    pks.get(1).getType shouldBe org.kududb.Type.BOOL

    //get nullable since no default in avro
    cols.get(2).isNullable shouldBe true
    cols.get(7).getDefaultValue.toString shouldBe "10.0"
  }

  "Should build a insert table cache" in {

    val table = mock[KuduTable]
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]

    when(client.tableExists(TABLE)).thenReturn(true)
    when(client.openTable(TABLE)).thenReturn(table)
    when(client.newSession()).thenReturn(kuduSession)

    val config = new KuduSinkConfig(getConfigAutoCreate(""))
    val settings = KuduSettings(config)
    val cache = DbHandler.buildTableCache(settings, client)
    cache(TOPIC) shouldBe table
  }

  "Should throw table not found when building insert cache" in {

    val table = mock[KuduTable]
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]

    //force table not found
    when(client.tableExists(TABLE)).thenReturn(false)
    when(client.openTable(TABLE)).thenReturn(table)
    when(client.newSession()).thenReturn(kuduSession)

    val config = new KuduSinkConfig(getConfig)
    val settings = KuduSettings(config)
    intercept[ConnectException] {
      DbHandler.buildTableCache(settings, client)
    }
  }

  "Should create table" in {
    val cluster = new EmbeddedSingleNodeKafkaCluster(1)
    cluster.start()
    val sr = new RestApp(8081, cluster.zKConnectString(), "_schemas")
    sr.start()
    Thread.sleep(3000)
    val srClient = new RestService("http://localhost:8081")
    val rawSchema: String =
      """
        |{"type":"record","name":"myrecord",
        |"fields":[
        |{"name":"firstName","type":["null", "string"]},
        |{"name":"lastName", "type": "string"},
        |{"name":"age", "type": "int"},
        |{"name":"bool", "type": "float"},
        |{"name":"byte", "type": "float"},
        |{"name":"short", "type": ["null", "int"]},
        |{"name":"long", "type": "long"},
        |{"name":"float", "type": "float"},
        |{"name":"double", "type": "double"}
        |]}";
      """.stripMargin

    //register the schema for topic1
    srClient.registerSchema(rawSchema, TOPIC)

    //set up configs
    val config = new KuduSinkConfig(getConfigAutoCreate("http://localhost:8081"))
    val settings = KuduSettings(config)

    //mock out kudu client
    val table = mock[KuduTable]
    val client = mock[KuduClient]

    val kuduSchemas = DbHandler.createTableProps(
      Set(rawSchema),
      settings.routes.head,
      config.getString(KuduSinkConfigConstants.SCHEMA_REGISTRY_URL),
      client)

    val kuduSchema = kuduSchemas.head.schema
    val cto = new CreateTableOptions
    val pks = settings.routes.head.getPrimaryKeys.asScala.toList.asJava
    cto.addHashPartitions(pks, 10)
    when(client.createTable(TABLE, kuduSchema, cto)).thenReturn(table)
    val ctp = CreateTableProps(TABLE, kuduSchema, cto)
    val ret: KuduTable = DbHandler.executeCreateTable(ctp, client)
    ret shouldBe table
  }

  "should alter table" in {
    //mock out kudu client
    val client = mock[KuduClient]
    val table = mock[KuduTable]
    val atrm = mock[AlterTableResponse]
    val ato = DbHandler.compare(createSchema, createSchema2).head
    when(client.tableExists(TABLE)).thenReturn(true)
    when(client.alterTable(TABLE, ato)).thenReturn(atrm)
    when(client.openTable(TABLE)).thenReturn(table)
    when(client.isAlterTableDone(TABLE)).thenReturn(true)
    val ret = DbHandler.alterTable(TABLE, createSchema, createSchema2, client)
    ret.isInstanceOf[KuduTable] shouldBe true
  }

  "should create table from sinkRecord" in {
    val client = mock[KuduClient]
    val record: SinkRecord = getTestRecords.head
    val config = new KuduSinkConfig(getConfigAutoCreate(""))
    val settings = KuduSettings(config)
    val ret = DbHandler.createTableFromSinkRecord(settings.routes.head, record.valueSchema(), client)
    ret.isInstanceOf[Try[KuduTable]] shouldBe true
  }
}
