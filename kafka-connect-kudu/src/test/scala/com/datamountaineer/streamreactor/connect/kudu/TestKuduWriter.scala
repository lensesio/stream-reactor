package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.streamreactor.connect.KuduConverter
import com.datamountaineer.streamreactor.connect.config.{KuduSettings, KuduSinkConfig}
import org.apache.kafka.connect.sink.SinkTaskContext
import org.kududb.client._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

/**
  * Created by andrew@datamountaineer.com on 04/03/16. 
  * stream-reactor
  */
class TestKuduWriter extends TestBase with KuduConverter with MockitoSugar {
  test("A Kudu Writer should write") {
    val record = getTestRecords.head
    val kuduSchema = convertToKuduSchema(record)
    val kuduRow = kuduSchema.newPartialRow()

    //mock out kudu client
    val insert = mock[Insert]
    when(insert.getRow).thenReturn(kuduRow)
    val table = mock[KuduTable]
    when(table.newInsert()).thenReturn(insert)
    val client = mock[KuduClient]
    when(client.openTable("sink_test")).thenReturn(table)
    val kuduSession = mock[KuduSession]
    //when(kuduSession.apply(insert))

    when(client.newSession()).thenReturn(kuduSession)

    val config = new KuduSinkConfig(getConfig)
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)

    val settings = KuduSettings(config, List(TOPIC), true)

    val writer = KuduWriter(config = config, settings = settings)
    //writer.write(getTestRecords)
    writer.close()
  }

}
