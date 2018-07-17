package com.landoop.streamreactor.connect.hive.parquet

import com.landoop.streamreactor.connect.hive.StructUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.scalatest.{Matchers, WordSpec}

class ParquetWriterTest extends WordSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(conf)

  "ParquetWriter" should {
    "write parquet files" in {

      val schema = SchemaBuilder.struct()
        .field("name", SchemaBuilder.string().required().build())
        .field("title", SchemaBuilder.string().optional().build())
        .field("salary", SchemaBuilder.float64().optional().build())
        .build()

      val users = List(
        new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43),
        new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06)
      )

      val path = new Path("sinktest.parquet")

      val writer = parquetWriter(path, schema, ParquetSinkConfig(overwrite = true))
      users.foreach(writer.write)
      writer.close()

      val reader = parquetReader(path)
      val actual = Iterator.continually(reader.read).takeWhile(_ != null).toList
      reader.close()

      actual.map(StructUtils.extractValues) shouldBe users.map(StructUtils.extractValues)

      fs.delete(path, false)
    }
    "support writing nulls" in {

      val schema = SchemaBuilder.struct()
        .field("name", SchemaBuilder.string().required().build())
        .field("title", SchemaBuilder.string().optional().build())
        .field("salary", SchemaBuilder.float64().optional().build())
        .build()

      val users = List(
        new Struct(schema).put("name", "sam").put("title", null).put("salary", 100.43),
        new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06)
      )

      val path = new Path("sinktest.parquet")

      val writer = parquetWriter(path, schema, ParquetSinkConfig(overwrite = true))
      users.foreach(writer.write)
      writer.close()

      val reader = parquetReader(path)
      val actual = Iterator.continually(reader.read).takeWhile(_ != null).toList
      reader.close()

      actual.map(StructUtils.extractValues) shouldBe users.map(StructUtils.extractValues)

      fs.delete(path, false)
    }
  }
}
