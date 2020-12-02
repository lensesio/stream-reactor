/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.config.{Format, FormatSelection}
import io.lenses.streamreactor.connect.aws.s3.model.{PartitionDisplay, PartitionField, PartitionSelection}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionedS3FileNamingStrategyTest extends AnyFlatSpec with Matchers {
  
  implicit val target = new PartitionedS3FileNamingStrategy(
    FormatSelection(Format.Json), 
    PartitionSelection(
      Seq(
        PartitionField("name"),
        PartitionField("title"),
        PartitionField("salary")
      ),
      PartitionDisplay.KeysAndValues
    ))

  
  def testRegex(path: String, topic: String, partition: Int, offset: Int): Unit = {
    path match {
      case CommittedFileName(regTopic, regPartition, regOffset, regFormat) =>
        regTopic.value should be(topic)
        regPartition should be(partition)
        regOffset.value should be(offset)
        regFormat should be(Format.Json)
      case _ => fail("Incorrect match")
    }
  }
    
  "regex" should "work for partitioned file" in {
    testRegex(
      "streamReactorBackups/name=first/title=primary/salary=[missing]/myTopic(1_50).json",
      "myTopic",
      1,
      50
    )
  }

  "regex" should "work for latest partitioned file" in {
    testRegex(
      "streamReactorBackups/name=first/title=primary/salary=[missing]/myTopic(1_latest_50).json",
      "myTopic",
      1,
      50
    )
  }
}
