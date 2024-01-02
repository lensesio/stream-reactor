/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.commit

import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.commit.ConditionCommitResult
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CountTest extends AnyFlatSpec with Matchers with EitherValues with MockitoSugar {

  private val count = Count(100)

  private def commitContext(currentCount: Long): CommitContext = {
    val cc = mock[CommitContext]
    when(cc.count).thenReturn(currentCount)
    cc
  }

  "count" should "return false when count not reached yet" in {
    count
      .eval(commitContext(99), debugEnabled = true) should
      be(ConditionCommitResult(commitTriggered = false, "count: '99/100'".some))
    count
      .eval(commitContext(99), debugEnabled = false) should
      be(ConditionCommitResult(commitTriggered = false, none))
  }

  "count" should "return true when count reached" in {
    count
      .eval(commitContext(100), debugEnabled = true) should
      be(ConditionCommitResult(commitTriggered = true, "count*: '100/100'".some))
    count
      .eval(commitContext(100), debugEnabled = false) should
      be(ConditionCommitResult(commitTriggered = true, none))
  }

  "count" should "return true when count exceeded" in {
    count
      .eval(commitContext(101), debugEnabled = true) should
      be(ConditionCommitResult(commitTriggered = true, "count*: '101/100'".some))
    count
      .eval(commitContext(101), debugEnabled = false) should
      be(ConditionCommitResult(commitTriggered = true, none))
  }

}
