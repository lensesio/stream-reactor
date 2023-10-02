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
package io.lenses.streamreactor.connect.aws.s3.model

import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.source.config.HierarchicalPartitionExtractor
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class HierarchicalPartitionExtractorTest extends AnyFlatSpecLike with Matchers with LazyLogging {

  private val hpe = new HierarchicalPartitionExtractor()

  "apply" should "parse a flattish path" in {
    val path = "topic/123/1.csv"
    hpe.extract(path) should be(123.some)
  }

  "apply" should "parse a path with long prefix" in {
    val path = "a/b/c/d/e/f/topic/456/1.json"
    hpe.extract(path) should be(456.some)
  }

  "apply" should "parse a path that starts with a slash" in {
    val path = "/topic/789/1.json"
    hpe.extract(path) should be(789.some)
  }

}
