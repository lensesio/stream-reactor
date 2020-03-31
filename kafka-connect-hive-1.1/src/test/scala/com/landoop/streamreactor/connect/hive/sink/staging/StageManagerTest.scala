package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive.{Offset, Topic, TopicPartition, TopicPartitionOffset}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StageManagerTest extends AnyWordSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(conf)

  val dir = new Path("stageman")
  fs.mkdirs(dir)

  val manager = new StageManager(DefaultFilenamePolicy)

  "StageManager" should {

    "stage file as hidden" in {
      val stagePath = manager.stage(dir, TopicPartition(Topic("mytopic"), 1))
      stagePath.getName.startsWith(".") shouldBe true
    }

    "delete existing file" in {

      val stagePath = manager.stage(dir, TopicPartition(Topic("mytopic"), 1))
      fs.create(stagePath)

      manager.stage(dir, TopicPartition(Topic("mytopic"), 1))
      fs.exists(stagePath) shouldBe false
    }
    "commit file using offset" in {

      val stagePath = manager.stage(dir, TopicPartition(Topic("mytopic"), 1))
      fs.create(stagePath)

      val tpo = TopicPartitionOffset(Topic("mytopic"), 1, Offset(100))
      val finalPath = manager.commit(stagePath, tpo)
      finalPath.getName shouldBe "streamreactor_mytopic_1_100"
    }
  }
}
