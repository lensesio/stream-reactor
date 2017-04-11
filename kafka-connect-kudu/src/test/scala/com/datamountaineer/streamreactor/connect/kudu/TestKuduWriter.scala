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

import com.datamountaineer.streamreactor.connect.kudu.config.{KuduSettings, KuduSinkConfig}
import com.datamountaineer.streamreactor.connect.kudu.sink.KuduWriter
import org.apache.kafka.connect.errors.RetriableException
import org.kududb.client._
import org.mockito.Matchers.{any, eq => mockEq}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 04/03/16. 
  * stream-reactor
  */
class TestKuduWriter extends TestBase with KuduConverter with MockitoSugar {
  "A Kudu Writer should write" in {
    val record = getTestRecords.head
    val kuduSchema = convertToKuduSchema(record)
    val kuduRow = kuduSchema.newPartialRow()

    //mock out kudu client
    val insert = mock[Upsert]
    val table = mock[KuduTable]
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]

    val config = new KuduSinkConfig(getConfigAutoCreate(""))
    val settings = KuduSettings(config)

    when(client.newSession()).thenReturn(kuduSession)
    when(client.tableExists(TABLE)).thenReturn(true)
    when(client.openTable(TABLE)).thenReturn(table)
    when(table.newUpsert()).thenReturn(insert)
    when(insert.getRow).thenReturn(kuduRow)
    when(table.getSchema).thenReturn(kuduSchema)

    val writer = new KuduWriter(client, settings)
    writer.write(getTestRecords)
    writer.close()
  }

  "A Kudu Writer should create table on arrival of first record" in {
    val record = getTestRecords.head
    val kuduSchema = convertToKuduSchema(record)
    val kuduRow = kuduSchema.newPartialRow()

    //mock out kudu client
    val insert = mock[Upsert]
    val table = mock[KuduTable]
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]

    val config = new KuduSinkConfig(getConfigAutoCreate(""))
    val settings = KuduSettings(config)

    when(client.newSession()).thenReturn(kuduSession)
    when(client.tableExists(TABLE)).thenReturn(false)
    when(client.openTable(TABLE)).thenReturn(table)
    when(table.newUpsert()).thenReturn(insert)
    when(insert.getRow).thenReturn(kuduRow)
    when(table.getSchema).thenReturn(kuduSchema)

    val writer = new KuduWriter(client, settings)
    writer.write(getTestRecords)
    writer.close()
  }

  "should identify schema change from source records" in {
    val schema1 = createSchema
    val schema2 = createSchema5

    val rec1 = createSinkRecord(createRecord(schema1, "1"), TOPIC, 1)
    val rec2 = createSinkRecord(createRecord5(schema2, "2"), TOPIC, 2)
    val kuduSchema = convertToKuduSchema(rec1)

    val kuduSchema2 = convertToKuduSchema(rec2.valueSchema())
    val kuduRow2 = kuduSchema2.newPartialRow()

    //mock out kudu client
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]
    val table = mock[KuduTable]
    val insert = mock[Upsert]
    val atrm = mock[AlterTableResponse]

    val config = new KuduSinkConfig(getConfigAutoCreateAndEvolve(""))
    val settings = KuduSettings(config)

    when(client.newSession()).thenReturn(kuduSession)
    when(client.tableExists(TABLE)).thenReturn(false)
    when(client.openTable(TABLE)).thenReturn(table)
    when(table.newUpsert()).thenReturn(insert)
    when(table.getSchema).thenReturn(kuduSchema)
    when(insert.getRow).thenReturn(kuduRow2)
    when(client.alterTable(mockEq(TABLE), any[AlterTableOptions])).thenReturn(atrm)
    when(client.isAlterTableDone(TABLE)).thenReturn(true)
    val writer = new KuduWriter(client, settings)

    writer.write(Set(rec1))
    writer.write(Set(rec2))
  }

  "A Kudu Writer should throw retry on flush errors" in {
    val record = getTestRecords.head
    val kuduSchema = convertToKuduSchema(record)
    val kuduRow = kuduSchema.newPartialRow()

    //mock out kudu client
    val insert = mock[Upsert]
    val table = mock[KuduTable]
    val client = mock[KuduClient]
    val kuduSession = mock[KuduSession]
    val resp = mock[OperationResponse]
    val errorRow = mock[RowError]

    val config = new KuduSinkConfig(getConfigAutoCreateRetry(""))
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
    when(kuduSession.flush()).thenReturn(List(resp))

    val writer = new KuduWriter(client, settings)

    intercept[RetriableException] {
      writer.write(getTestRecords)
    }
  }
}
