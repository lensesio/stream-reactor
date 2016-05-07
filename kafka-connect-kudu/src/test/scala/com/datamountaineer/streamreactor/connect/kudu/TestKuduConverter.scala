package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.datamountaineer.streamreactor.connect.KuduConverter
import org.kududb.client.{Insert, KuduTable}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

/**
  * Created by andrew@datamountaineer.com on 04/03/16. 
  * stream-reactor
  */
class TestKuduConverter extends TestBase with KuduConverter with ConverterUtil with MockitoSugar{
  test("Should convert a SinkRecord Schema to Kudu Schema") {
    val record = getTestRecords.head
    val connectSchema = record.valueSchema()
    val connectFields = connectSchema.fields()
    val kuduSchema = convertToKuduSchema(record)

    val columns = kuduSchema.getColumns
    columns.get(0).getName shouldBe connectFields.get(0).name()
    columns.get(1).getName shouldBe connectFields.get(1).name()
    columns.get(2).getName shouldBe connectFields.get(2).name()
    columns.get(3).getName shouldBe connectFields.get(3).name()
  }

  test("Should convert a SinkRecord into a Kudu Insert operation") {
    val record = getTestRecords.head
    val kuduSchema = convertToKuduSchema(record)
    val kuduRow = kuduSchema.newPartialRow()
    val insert = mock[Insert]
    when(insert.getRow).thenReturn(kuduRow)
    val table = mock[KuduTable]
    when(table.newInsert()).thenReturn(insert)
    val kuduRecord = convert(record, table)
    //??? not really a test
    kuduRecord.getRow shouldBe kuduRow
  }
}
