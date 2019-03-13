package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive.{Offset, Topic, TopicPartitionOffset}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}
import org.scalatest.{Matchers, WordSpec}
import scala.concurrent.duration._

class DefaultCommitPolicyTest extends WordSpec with Matchers {

  val schema = SchemaBuilder.struct()
    .field("name", SchemaBuilder.string().required().build())
    .build()

  val struct = new Struct(schema)

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(conf)
  val tpo = TopicPartitionOffset(Topic("mytopic"), 1, Offset(100))

  "DefaultCommitPolicy" should {
    "roll over after interval" in {

      val policy = DefaultCommitPolicy(None, Option(2.seconds), None)
      val path = new Path("foo")
      fs.create(path)

      policy.shouldFlush(struct, tpo, path, 10) shouldBe false
      Thread.sleep(2000)
      policy.shouldFlush(struct, tpo, path, 10) shouldBe true

      fs.delete(path, false)
    }
    "roll over after file count" in {
      val policy = DefaultCommitPolicy(None, None, Some(9))
      val path = new Path("foo")
      fs.create(path)

      policy.shouldFlush(struct, tpo, path, 7) shouldBe false
      policy.shouldFlush(struct, tpo, path, 8) shouldBe false
      policy.shouldFlush(struct, tpo, path, 9) shouldBe true
      policy.shouldFlush(struct, tpo, path, 10) shouldBe true

      fs.delete(path, false)
    }
    "roll over after file size" in {
      val policy = DefaultCommitPolicy(Some(10), None, None)
      val path = new Path("foo")
      val out = fs.create(path)
      policy.shouldFlush(struct, tpo, path, 7) shouldBe false
      out.writeBytes("wibble wobble wabble wubble")
      out.close()
      policy.shouldFlush(struct, tpo, path, 9) shouldBe true
      fs.delete(path, false)
    }
  }
}
