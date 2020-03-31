package com.landoop.streamreactor.connect.hive.orc

import com.landoop.streamreactor.connect.hive.{OrcSinkConfig, OrcSourceConfig, StructUtils, orc}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OrcTest extends AnyFlatSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(conf)

  "Orc" should "read and write orc files" in {

    val schema = SchemaBuilder.struct()
      .field("name", SchemaBuilder.string().optional().build())
      .field("age", SchemaBuilder.int32().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .name("from_orc")
      .build()

    val users = Seq(
      new Struct(schema).put("name", "sammy").put("age", 38).put("salary", 54.67),
      new Struct(schema).put("name", "laura").put("age", 37).put("salary", 91.84)
    )

    val path = new Path("orctest.orc")
    val sink = orc.sink(path, schema, OrcSinkConfig(overwrite = true))
    users.foreach(sink.write)
    sink.close()

    val source = orc.source(path, OrcSourceConfig())
    val actual = source.iterator.toList
    actual.head.schema shouldBe schema
    actual.map(StructUtils.extractValues) shouldBe
      List(Vector("sammy", 38, 54.67), Vector("laura", 37, 91.84))

    fs.delete(path, false)
  }
}