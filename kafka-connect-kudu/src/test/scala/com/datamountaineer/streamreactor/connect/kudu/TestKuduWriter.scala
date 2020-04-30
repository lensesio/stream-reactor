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

import java.util.Collections

import com.datamountaineer.kcql.{Bucketing, Kcql}
import com.datamountaineer.streamreactor.connect.kudu.config.{KuduConfig, KuduSettings}
import com.datamountaineer.streamreactor.connect.kudu.sink.KuduWriter
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.RetriableException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.kudu.client._
import org.mockito.ArgumentMatchers.{any, eq => mockEq}
import org.mockito.MockitoSugar

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 04/03/16.
  * stream-reactor
  */
class TestKuduWriter extends TestBase with KuduConverter with MockitoSugar with ConverterUtil {
  "A Kudu Writer should write" in {
    val kcql = mock[Kcql]
    val bucketing = mock[Bucketing]
    when(bucketing.getBucketNames).thenReturn(Collections.emptyIterator[String]())
    when(kcql.getBucketing).thenReturn(bucketing)
    val record = getTestRecords.head

    val kuduSchema = convertToKuduSchema(record, kcql)
    val kuduRow = kuduSchema.newPartialRow()

    //mock out kudu client
    val insert = mock[Upsert]
    val table = mock[KuduTable]
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]
    val resp = mock[ListTablesResponse]

    val config = new KuduConfig(getConfigAutoCreate(""))
    val settings = KuduSettings(config)

    when(client.newSession()).thenReturn(kuduSession)
    when(client.tableExists(TABLE)).thenReturn(true)
    when(client.openTable(TABLE)).thenReturn(table)
    when(table.newUpsert()).thenReturn(insert)
    when(insert.getRow).thenReturn(kuduRow)
    when(table.getSchema).thenReturn(kuduSchema)
    when(kuduSession.getFlushMode).thenReturn(FlushMode.AUTO_FLUSH_SYNC)
    when(client.getTablesList).thenReturn(resp)
    when(resp.getTablesList).thenReturn(List.empty[String].asJava)

    val writer = new KuduWriter(client, settings)
    writer.write(getTestRecords.toSeq)
    writer.close()
  }

  "A Kudu writer should write JSON" in {
    val jsonPayload =
      """
        | {
        |    "_id": "580151bca6f3a2f0577baaac",
        |    "index": 0,
        |    "guid": "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba",
        |    "isActive": false,
        |    "balance": 3589.15,
        |    "age": 27,
        |    "eyeColor": "brown",
        |    "name": "Clements Crane",
        |    "company": "TERRAGEN",
        |    "email": "clements.crane@terragen.io",
        |    "phone": "+1 (905) 514-3719",
        |    "address": "316 Hoyt Street, Welda, Puerto Rico, 1474",
        |    "latitude": "-49.817964",
        |    "longitude": "-141.645812"
        | }
      """.stripMargin

    val topic = "sink_test"
    val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

    val payload = convertStringSchemaAndJson(record, Map.empty, Set.empty)

    val kuduSchema = convertToKuduSchemaFromJson(payload, topic)
    val kuduRow = kuduSchema.newPartialRow()

    //mock out kudu client
    val insert = mock[Upsert]
    val table = mock[KuduTable]
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]
    val resp = mock[ListTablesResponse]

    val config = new KuduConfig(getConfigAutoCreate(""))
    val settings = KuduSettings(config)

    when(client.newSession()).thenReturn(kuduSession)
    when(client.tableExists(TABLE)).thenReturn(true)
    when(client.openTable(TABLE)).thenReturn(table)
    when(table.newUpsert()).thenReturn(insert)
    when(insert.getRow).thenReturn(kuduRow)
    when(table.getSchema).thenReturn(kuduSchema)
    when(kuduSession.getFlushMode).thenReturn(FlushMode.AUTO_FLUSH_SYNC)
    when(client.getTablesList).thenReturn(resp)
    when(resp.getTablesList).thenReturn(List.empty[String].asJava)

    val writer = new KuduWriter(client, settings)
    writer.write(Seq(record))
    writer.close()
  }


  "A Kudu Writer should create table on arrival of first record" in {
    val kcql = mock[Kcql]
    val bucketing = mock[Bucketing]
    when(bucketing.getBucketNames).thenReturn(Collections.emptyIterator[String]())
    when(kcql.getBucketing).thenReturn(bucketing)
    val record = getTestRecords.head
    val kuduSchema = convertToKuduSchema(record, kcql)
    val kuduRow = kuduSchema.newPartialRow()

    //mock out kudu client
    val insert = mock[Upsert]
    val table = mock[KuduTable]
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]
    val resp = mock[ListTablesResponse]

    val config = new KuduConfig(getConfigAutoCreate(""))
    val settings = KuduSettings(config)

    when(client.newSession()).thenReturn(kuduSession)
    when(client.tableExists(TABLE)).thenReturn(false)
    when(client.openTable(TABLE)).thenReturn(table)
    when(table.newUpsert()).thenReturn(insert)
    when(insert.getRow).thenReturn(kuduRow)
    when(table.getSchema).thenReturn(kuduSchema)
    when(kuduSession.getFlushMode).thenReturn(FlushMode.AUTO_FLUSH_SYNC)
    when(client.getTablesList).thenReturn(resp)
    when(resp.getTablesList).thenReturn(List.empty[String].asJava)


    val writer = new KuduWriter(client, settings)
    writer.write(getTestRecords.toSeq)
    writer.close()
  }

  "A Kudu Writer should create table on arrival of first JSON record" in {
    val jsonPayload =
      """
        | {
        |    "_id": "580151bca6f3a2f0577baaac",
        |    "index": 0,
        |    "guid": "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba",
        |    "isActive": false,
        |    "balance": 3589.15,
        |    "age": 27,
        |    "eyeColor": "brown",
        |    "name": "Clements Crane",
        |    "company": "TERRAGEN",
        |    "email": "clements.crane@terragen.io",
        |    "phone": "+1 (905) 514-3719",
        |    "address": "316 Hoyt Street, Welda, Puerto Rico, 1474",
        |    "latitude": "-49.817964",
        |    "longitude": "-141.645812"
        | }
      """.stripMargin

    val topic = "sink_test"
    val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

    val payload = convertStringSchemaAndJson(record, Map.empty, Set.empty)

    val kuduSchema = convertToKuduSchemaFromJson(payload, topic)
    val kuduRow = kuduSchema.newPartialRow()

    //mock out kudu client
    val insert = mock[Upsert]
    val table = mock[KuduTable]
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]
    val resp = mock[ListTablesResponse]

    val config = new KuduConfig(getConfigAutoCreate(""))
    val settings = KuduSettings(config)

    when(client.newSession()).thenReturn(kuduSession)
    when(client.tableExists(TABLE)).thenReturn(false)
    when(client.openTable(TABLE)).thenReturn(table)
    when(table.newUpsert()).thenReturn(insert)
    when(insert.getRow).thenReturn(kuduRow)
    when(table.getSchema).thenReturn(kuduSchema)
    when(kuduSession.getFlushMode).thenReturn(FlushMode.AUTO_FLUSH_SYNC)
    when(client.getTablesList).thenReturn(resp)
    when(resp.getTablesList).thenReturn(List.empty[String].asJava)

    val writer = new KuduWriter(client, settings)
    writer.write(getTestRecords.toSeq)
    writer.close()
  }

  "should identify schema change from source records" in {
    val kcql = mock[Kcql]
    val bucketing = mock[Bucketing]
    when(bucketing.getBucketNames).thenReturn(Collections.emptyIterator[String]())
    when(kcql.getBucketing).thenReturn(bucketing)
    val schema1 = createSchema
    val schema2 = createSchema5

    val rec1 = createSinkRecord(createRecord(schema1, "1"), TOPIC, 1)
    val rec2 = createSinkRecord(createRecord5(schema2, "2"), TOPIC, 2)
    val kuduSchema = createKuduSchema

    val kuduSchema2 = createKuduSchema5
    val kuduRow2 = kuduSchema2.newPartialRow()

    //mock out kudu client
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]
    val table = mock[KuduTable]
    val insert = mock[Upsert]
    val atrm = mock[AlterTableResponse]
    val resp = mock[ListTablesResponse]

    val config = new KuduConfig(getConfigAutoCreateAndEvolve(""))
    val settings = KuduSettings(config)

    when(client.newSession()).thenReturn(kuduSession)
    when(client.tableExists(TABLE)).thenReturn(false)
    when(client.openTable(TABLE)).thenReturn(table)
    when(table.newUpsert()).thenReturn(insert)
    when(table.getSchema).thenReturn(kuduSchema)
    when(insert.getRow).thenReturn(kuduRow2)
    when(client.alterTable(mockEq(TABLE), any[AlterTableOptions])).thenReturn(atrm)
    when(client.isAlterTableDone(TABLE)).thenReturn(true)
    when(kuduSession.getFlushMode).thenReturn(FlushMode.AUTO_FLUSH_SYNC)
    when(client.getTablesList).thenReturn(resp)
    when(resp.getTablesList).thenReturn(List.empty[String].asJava)

    val writer = new KuduWriter(client, settings)

    writer.write(Seq(rec1))
    writer.write(Seq(rec2))
  }

  "A Kudu Writer should throw retry on flush errors" in {
    val kcql = mock[Kcql]
    val bucketing = mock[Bucketing]
    when(bucketing.getBucketNames).thenReturn(Collections.emptyIterator[String]())
    when(kcql.getBucketing).thenReturn(bucketing)
    val record = getTestRecords.head
    val kuduSchema = convertToKuduSchema(record, kcql)
    val kuduRow = kuduSchema.newPartialRow()

    //mock out kudu client
    val insert = mock[Upsert]
    val table = mock[KuduTable]
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]
    val resp = mock[OperationResponse]
    val errorRow = mock[RowError]
    val tableresp = mock[ListTablesResponse]

    val config = new KuduConfig(getConfigAutoCreateRetry(""))
    val settings = KuduSettings(config)

    when(client.newSession()).thenReturn(kuduSession)
    when(client.tableExists(TABLE)).thenReturn(true)
    when(client.openTable(TABLE)).thenReturn(table)
    when(table.newUpsert()).thenReturn(insert)
    when(table.getSchema).thenReturn(kuduSchema)
    when(insert.getRow).thenReturn(kuduRow)
    when(resp.hasRowError).thenReturn(true)
    when(errorRow.toString).thenReturn("Test error string")
    when(resp.getRowError).thenReturn(errorRow)
    when(kuduSession.flush()).thenReturn(List(resp).asJava)
    when(kuduSession.getFlushMode).thenReturn(FlushMode.AUTO_FLUSH_SYNC)
    when(client.getTablesList).thenReturn(tableresp)
    when(tableresp.getTablesList).thenReturn(List.empty[String].asJava)

    val writer = new KuduWriter(client, settings)

    intercept[RetriableException] {
      writer.write(getTestRecords.toSeq)
    }
  }

  "A Kudu Writer should check pending errors and throw exception" in {
    val kcql = mock[Kcql]
    val bucketing = mock[Bucketing]
    when(bucketing.getBucketNames).thenReturn(Collections.emptyIterator[String]())
    when(kcql.getBucketing).thenReturn(bucketing)
    val record = getTestRecords.head
    val kuduSchema = convertToKuduSchema(record, kcql)
    val kuduRow = kuduSchema.newPartialRow()

    //mock out kudu client
    val insert = mock[Upsert]
    val table = mock[KuduTable]
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]
    val errorRow = mock[RowError]
    val rowErrorsAndOverflowStatus = mock[RowErrorsAndOverflowStatus]
    val resp = mock[ListTablesResponse]
    when(rowErrorsAndOverflowStatus.getRowErrors()).thenReturn(Array[RowError](errorRow))

    val config = new KuduConfig(getConfigAutoCreateRetryWithBackgroundFlush(""))
    val settings = KuduSettings(config)

    when(client.newSession()).thenReturn(kuduSession)
    when(client.tableExists(TABLE)).thenReturn(true)
    when(client.openTable(TABLE)).thenReturn(table)
    when(table.newUpsert()).thenReturn(insert)
    when(table.getSchema).thenReturn(kuduSchema)
    when(insert.getRow).thenReturn(kuduRow)
    when(errorRow.toString).thenReturn("Test error string")
    when(kuduSession.getFlushMode).thenReturn(FlushMode.AUTO_FLUSH_BACKGROUND)
    when(kuduSession.getPendingErrors()).thenReturn(rowErrorsAndOverflowStatus)
    when(client.getTablesList).thenReturn(resp)
    when(resp.getTablesList).thenReturn(List.empty[String].asJava)


    val writer = new KuduWriter(client, settings)
    verify(kuduSession, times(1)).setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
    intercept[RetriableException] {
      writer.write(getTestRecords.toSeq)
    }
    verify(kuduSession, times(1)).getPendingErrors

  }
}
