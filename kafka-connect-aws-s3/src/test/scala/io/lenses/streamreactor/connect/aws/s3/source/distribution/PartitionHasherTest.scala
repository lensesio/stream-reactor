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
package io.lenses.streamreactor.connect.aws.s3.source.distribution

import io.lenses.streamreactor.connect.cloud.source.config.distribution.PartitionHasher
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class PartitionHasherTest extends AnyFlatSpecLike with Matchers {

  "partitionHasher" should "distribute hashes evenly over large number of tasks" in {
    testHashDistribution(
      numberOfValues = 1000,
      maxTasks       = 500,
      topics         = Seq("/myTopic/", "/anotherTopic/", "/thirdTopic/"),
    )
  }

  "partitionHasher" should "distribute hashes evenly over small number of tasks" in {
    testHashDistribution(
      numberOfValues = 1000,
      maxTasks       = 10,
      topics         = Seq("/myTopic/"),
    )
  }

  "partitionHasher" should "not distribute hashes over single task" in {
    mapPartitionsToRange(numberOfValues = 1000,
                         maxTasks       = 1,
                         topics         = Seq("/myTopic/", "/anotherTopic/", "/thirdTopic/"),
    ).foreach(mp => mp should be(0))
  }

  private def testHashDistribution(numberOfValues: Int, maxTasks: Int, topics: Seq[String]) = {
    val topicsMappedToPartitions: Seq[Int] = mapPartitionsToRange(numberOfValues, maxTasks, topics)

    val grouped: Map[Int, Int] = topicsMappedToPartitions.groupBy(identity).view.mapValues(_.size).toMap

    grouped.size should be(maxTasks)
    grouped.foreach(gp => shouldBeInRange(gp._2, (numberOfValues * topics.size * 2) / maxTasks))

    grouped.values.sum should be(numberOfValues * topics.size)
  }

  private def shouldBeInRange(count: Int, upperBounds: Int): Assertion = {
    count should be > 0
    count should be <= upperBounds
  }

  private def mapPartitionsToRange(numberOfValues: Int, maxTasks: Int, topics: Seq[String]): Seq[Int] = {
    val rangeToTest = 1 to numberOfValues
    val partitions  = topics.flatMap(t => rangeToTest.map(t + _))

    partitions.map(PartitionHasher.hash(maxTasks, _))
  }
}
