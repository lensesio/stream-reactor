package com.datamountaineer.streamreactor.connect.hbase.writers

import java.util.UUID

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.json4s.DefaultFormats
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import com.datamountaineer.streamreactor.connect.hbase.BytesHelper._
import com.datamountaineer.streamreactor.connect.hbase.{AvroRecordRowKeyBuilder, FieldsValuesExtractor, HbaseHelper, HbaseTableHelper}
import org.kitesdk.minicluster.Service.ServiceConfig
import org.kitesdk.minicluster._

import scala.reflect.io.File

class HbaseWriterTest extends WordSpec with Matchers with MockitoSugar with BeforeAndAfter {


  implicit val formats = DefaultFormats

  before {
    val workDir = "target/kite-minicluster-workdir-hbase"
    val miniCluster = new MiniCluster
                            .Builder()
                            .workDir(workDir)
                            .bindIP("localhost")
                            .zkPort(2181)
                            .addService(classOf[HdfsService])
                            .addService(classOf[ZookeeperService])
                            .addService(classOf[HBaseService])
                            .clean(true).build
    miniCluster.start()
    //Thread.sleep(5000)
  }

//  after {
//    File("target/kite-minicluster-workdir-hbase").deleteRecursively()
//  }


  "HbaseWriter" should {

    "write an Hbase row for each SinkRecord provided" in {

      val colFam = "personal"
      val table = s"user${System.currentTimeMillis()}"


      val fieldsExtractor = mock[FieldsValuesExtractor]
      val rowKeyBuilder = mock[AvroRecordRowKeyBuilder]

      val writer = new HbaseWriter(
        colFam,
        table,
        fieldsExtractor,
        rowKeyBuilder)

      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("firstName", "Alex").put("age", 30)
      val struct2 = new Struct(schema).put("firstName", "Mara").put("age", 22).put("threshold", 12.4)

      val sinkRecord1 = new SinkRecord("topic", 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord("topic", 1, null, null, schema, struct2, 1)


      when(rowKeyBuilder.build(sinkRecord1, null)).thenReturn(10.fromInt())
      when(rowKeyBuilder.build(sinkRecord2, null)).thenReturn(11.fromInt())

      when(fieldsExtractor.get(struct1)).thenReturn(Seq("firstName" -> "Alex".fromString(), "age" -> 30.fromInt()))
      when(fieldsExtractor.get(struct2)).thenReturn(Seq("firstName" -> "Mara".fromString(), "age" -> 22.fromInt(), "threshold" -> 12.4.fromDouble()))

      HbaseHelper.autoclose(HbaseReaderHelper.createConnection) { connection =>
        implicit val conn = connection
        try {
          HbaseTableHelper.createTable(table, colFam)
          writer.write(Seq(sinkRecord1, sinkRecord2))

          val data = HbaseReaderHelper.getAllRecords(table, colFam)

          data.size shouldBe 2

          val row1 = data.filter { r => Bytes.toInt(r.key) == 10 }.head
          row1.cells.size shouldBe 2

          Bytes.toString(row1.cells.get("firstName").get) shouldBe "Alex"
          Bytes.toInt(row1.cells.get("age").get) shouldBe 30


          val row2 = data.filter { r => Bytes.toInt(r.key) == 11 }.head
          row2.cells.size shouldBe 3

          Bytes.toString(row2.cells.get("firstName").get) shouldBe "Mara"
          Bytes.toInt(row2.cells.get("age").get) shouldBe 22
          Bytes.toDouble(row2.cells.get("threshold").get) shouldBe 12.4

        }
        finally {
          HbaseTableHelper.deleteTable(table)
        }
      }
    }
  }
}