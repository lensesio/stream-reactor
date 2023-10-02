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
package io.lenses.streamreactor.connect.cloud.common.utils

import io.lenses.streamreactor.connect.cloud.common.utils.IteratorOps
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class IteratorOpsTest extends AnyFlatSpecLike with Matchers {
  "IteratorOps" should "raise NoSuchElementException" in {
    val iterator = Iterator.empty[Int]
    IteratorOps.skip(iterator, 1) match {
      case Left(value: NoSuchElementException) => value.getMessage shouldBe "No more items at index 0"
      case Left(value) => fail(s"Should be NoSuchElementException but was $value")
      case Right(_)    => fail("Should not be right")
    }
  }
  "IteratorOps" should "skip one item in a 1 item iterator" in {
    val iterator = Iterator(1)
    IteratorOps.skip(iterator, 1) match {
      case Left(err) => fail(s"Should not be left. $err")
      case Right(_)  => iterator.hasNext shouldBe false
    }
  }
  "IteratorOps" should "skip two items in a 3 items iterator" in {
    val iterator = Iterator(1, 2, 3)
    IteratorOps.skip(iterator, 2) match {
      case Left(err) => fail(s"Should not be left. $err")
      case Right(_) => iterator.hasNext shouldBe true
        iterator.next() shouldBe 3
    }
  }
  "IteratorOps" should "skip more than the items in it" in {
    val iterator = Iterator(1, 2, 3)
    IteratorOps.skip(iterator, 4) match {
      case Left(err) => err.getMessage shouldBe "No more items at index 3"
      case Right(_)  => fail("Should not be right")
    }
  }
}
