package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive.{Offset, Topic, TopicPartitionOffset}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class DefaultCommitPolicyTest extends AnyWordSpec with Matchers {

  val schema: Schema = SchemaBuilder.struct()
    .field("name", SchemaBuilder.string().required().build())
    .build()

  val struct = new Struct(schema)

  implicit val conf: Configuration = new Configuration()
  implicit val fs: LocalFileSystem = FileSystem.getLocal(conf)
  val tpo = TopicPartitionOffset(Topic("mytopic"), 1, Offset(100))

  private def shouldFlush(policy: CommitPolicy, path: Path, count: Long) = {
    val status = fs.getFileStatus(path)
    policy.shouldFlush(CommitContext(tpo, path, count, status.getLen, status.getModificationTime))
  }

  "DefaultCommitPolicy" should {
    "roll over after interval" in {

      val policy = DefaultCommitPolicy(None, Option(2.seconds), None)
      val path = new Path("foo")
      fs.create(path)

      shouldFlush(policy, path, 10) shouldBe false
      Thread.sleep(2000)
      shouldFlush(policy, path, 10) shouldBe true

      fs.delete(path, false)
    }
    "roll over after file count" in {
      val policy = DefaultCommitPolicy(None, None, Some(9))
      val path = new Path("foo")
      fs.create(path)

      shouldFlush(policy, path, 7) shouldBe false
      shouldFlush(policy, path, 8) shouldBe false
      shouldFlush(policy, path, 9) shouldBe true
      shouldFlush(policy, path, 10) shouldBe true

      fs.delete(path, false)
    }
    "roll over after file size" in {
      val policy = DefaultCommitPolicy(Some(10), None, None)
      val path = new Path("foo")
      val out = fs.create(path)
      shouldFlush(policy, path, 7) shouldBe false
      out.writeBytes("wibble wobble wabble wubble")
      out.close()
      shouldFlush(policy, path, 9) shouldBe true
      fs.delete(path, false)
    }
  }
}
