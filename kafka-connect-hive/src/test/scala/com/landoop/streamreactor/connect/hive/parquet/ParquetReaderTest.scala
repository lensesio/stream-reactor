package com.landoop.streamreactor.connect.hive.parquet

import com.landoop.streamreactor.connect.hive.StructUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.connect.data.SchemaBuilder
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

class ParquetReaderTest extends AnyWordSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(conf)

  "ParquetReader" should {
    "read parquet files" in {

      val path = new Path("file://" + getClass.getResource("/userdata.parquet").getFile)
      val reader = parquetReader(path)
      val users = Iterator.continually(reader.read).takeWhile(_ != null).toList

      val schema = SchemaBuilder.struct()
        .field("registration_dttm", SchemaBuilder.string().optional().build())
        .field("id", SchemaBuilder.int32().optional().build())
        .field("first_name", SchemaBuilder.string().optional().build())
        .field("last_name", SchemaBuilder.string().optional().build())
        .field("email", SchemaBuilder.string().optional().build())
        .field("gender", SchemaBuilder.string().optional().build())
        .field("ip_address", SchemaBuilder.string().optional().build())
        .field("cc", SchemaBuilder.string().optional().build())
        .field("country", SchemaBuilder.string().optional().build())
        .field("birthdate", SchemaBuilder.string().optional().build())
        .field("salary", SchemaBuilder.float64().optional().build())
        .field("title", SchemaBuilder.string().optional().build())
        .field("comments", SchemaBuilder.string().optional().build())
        .name("hive_schema")
        .build()

      users.head.schema().fields().asScala.map(_.name) shouldBe schema.fields().asScala.map(_.name)

      StructUtils.extractValues(users.head).tail shouldBe Vector(
        1,
        "Amanda",
        "Jordan",
        "ajordan0@com.com",
        "Female",
        "1.197.201.2",
        "6759521864920116",
        "Indonesia",
        "3/8/1971",
        49756.53,
        "Internal Auditor",
        "1E+02"
      )
    }
  }
}

//          StructField("registration_dttm", TimestampDataType),
//          StructField("id", IntDataType),
//          StructField("first_name", StringDataType),
//          StructField("last_name", StringDataType),
//          StructField("email", StringDataType),
//          StructField("gender", StringDataType),
//          StructField("ip_address", StringDataType),
//          StructField("cc", StringDataType),
//          StructField("country", StringDataType),
//          StructField("birthdate", StringDataType),
//          StructField("salary", DoubleDataType),
//          StructField("title", StringDataType),
//          StructField("comments", StringDataType)
