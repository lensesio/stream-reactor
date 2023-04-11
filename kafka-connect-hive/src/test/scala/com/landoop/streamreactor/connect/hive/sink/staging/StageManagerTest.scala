/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.landoop.streamreactor.connect.hive.sink.staging

import com.landoop.streamreactor.connect.hive.Offset
import com.landoop.streamreactor.connect.hive.Topic
import com.landoop.streamreactor.connect.hive.TopicPartition
import com.landoop.streamreactor.connect.hive.TopicPartitionOffset
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StageManagerTest extends AnyWordSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs   = FileSystem.getLocal(conf)

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

      val tpo       = TopicPartitionOffset(Topic("mytopic"), 1, Offset(100))
      val finalPath = manager.commit(stagePath, tpo)
      finalPath.getName shouldBe "streamreactor_mytopic_1_100"
    }
  }
}
