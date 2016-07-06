package com.datamountaineer.streamreactor.connect.hbase.writers

import com.datamountaineer.streamreactor.connect.hbase.config.HbaseSinkConfig._
import com.datamountaineer.streamreactor.connect.hbase.config.{HbaseSettings, HbaseSinkConfig}
import com.datamountaineer.streamreactor.connect.hbase.{HbaseHelper, HbaseTableHelper, StructFieldsRowKeyBuilderBytes}
import com.datamountaineer.streamreactor.connect.hbase.BytesHelper._
import com.datamountaineer.streamreactor.connect.hbase.FieldsValuesExtractor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.errors.RetriableException
import org.apache.kafka.connect.sink.SinkRecord
import org.json4s.DefaultFormats
import org.kitesdk.minicluster._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

class HbaseWriterTestRetry extends WordSpec with Matchers with MockitoSugar with BeforeAndAfter {

  implicit val formats = DefaultFormats
  var miniCluster : Option[MiniCluster] = None

  before {
    val workDir = "target/kite-minicluster-workdir-hbase"
    miniCluster = Some(new MiniCluster
                            .Builder()
                            .workDir(workDir)
                            .bindIP("localhost")
                            .zkPort(2181)
                            .addService(classOf[HdfsService])
                            .addService(classOf[ZookeeperService])
                            .addService(classOf[HBaseService])
                            .clean(true).build)
    miniCluster.get.start()
  }

  after {
    miniCluster.get.stop()
  }

  "HbaseWriter" should {

    "write an Hbase row for each SinkRecord provided using StructFieldsRowKeyBuilderBytes" in {

      val fieldsExtractor = mock[FieldsValuesExtractor]
      val rowKeyBuilder = mock[StructFieldsRowKeyBuilderBytes]

      val config = mock[HbaseSinkConfig]
      val tableName = "someTable"
      val topic = "someTopic"
      val columnFamily = "somecolumnFamily"
      val QUERY_ALL = s"INSERT INTO $tableName SELECT * FROM $topic PK firstName"

      when(config.getString(COLUMN_FAMILY)).thenReturn(columnFamily)
      when(config.getString(EXPORT_ROUTE_QUERY)).thenReturn(QUERY_ALL)
      when(config.getString(HbaseSinkConfig.ERROR_POLICY)).thenReturn("RETRY")
      when(config.getInt(HbaseSinkConfig.NBR_OF_RETRIES)).thenReturn(HbaseSinkConfig.NBR_OF_RETIRES_DEFAULT)

      val settings = HbaseSettings(config)

      val writer = new HbaseWriter(settings)

      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("firstName", "Alex").put("age", 30)
      val struct2 = new Struct(schema).put("firstName", "Mara").put("age", 22).put("threshold", 12.4)

      val sinkRecord1 = new SinkRecord(topic, 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord(topic, 1, null, null, schema, struct2, 1)

      when(rowKeyBuilder.build(sinkRecord1, null)).thenReturn(10.fromInt())
      when(rowKeyBuilder.build(sinkRecord2, null)).thenReturn(11.fromInt())

      when(fieldsExtractor.get(struct1)).thenReturn(Seq("firstName" -> "Alex".fromString(), "age" -> 30.fromInt()))
      when(fieldsExtractor.get(struct2)).thenReturn(Seq("firstName" -> "Mara".fromString(), "age" -> 22.fromInt(), "threshold" -> 12.4.fromDouble()))

      HbaseHelper.autoclose(HbaseReaderHelper.createConnection) { connection =>
        implicit val conn = connection
        try {
          //HbaseTableHelper.createTable(tableName, columnFamily)

          //write should now error and retry

          intercept[RetriableException] {
            writer.write(Seq(sinkRecord1, sinkRecord2))
          }

          HbaseTableHelper.createTable(tableName, columnFamily)

          //write again, should recover
          writer.write(Seq(sinkRecord1, sinkRecord2))

          val data = HbaseReaderHelper.getAllRecords(tableName, columnFamily)

          data.size shouldBe 2

          val row1 = data.filter { r => Bytes.toString(r.key) == "Alex" }.head
          row1.cells.size shouldBe 2

          Bytes.toString(row1.cells.get("firstName").get) shouldBe "Alex"
          Bytes.toInt(row1.cells.get("age").get) shouldBe 30


          val row2 = data.filter { r => Bytes.toString(r.key) == "Mara" }.head
          row2.cells.size shouldBe 3

          Bytes.toString(row2.cells.get("firstName").get) shouldBe "Mara"
          Bytes.toInt(row2.cells.get("age").get) shouldBe 22
          Bytes.toDouble(row2.cells.get("threshold").get) shouldBe 12.4

        }
        finally {
          HbaseTableHelper.deleteTable(tableName)
        }
      }
    }
  }
}