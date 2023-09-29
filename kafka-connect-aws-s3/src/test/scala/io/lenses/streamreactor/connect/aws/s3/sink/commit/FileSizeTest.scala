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
package io.lenses.streamreactor.connect.aws.s3.sink.commit

import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.cloud.sink.commit.CommitContext
import io.lenses.streamreactor.connect.cloud.sink.commit.ConditionCommitResult
import io.lenses.streamreactor.connect.cloud.sink.commit.FileSize
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileSizeTest extends AnyFlatSpec with Matchers with EitherValues with MockitoSugar {

  private val fileSize = FileSize(100)

  private def commitContext(currentFileSize: Long): CommitContext = {
    val cc = mock[CommitContext]
    when(cc.fileSize).thenReturn(currentFileSize)
    cc
  }

  "fileSize" should "return false when fileSize not reached yet" in {
    fileSize
      .eval(commitContext(99), debugEnabled = true) should
      be(ConditionCommitResult(commitTriggered = false, "fileSize: '99/100'".some))
    fileSize
      .eval(commitContext(99), debugEnabled = false) should
      be(ConditionCommitResult(commitTriggered = false, none))
  }

  "fileSize" should "return true when fileSize reached" in {
    fileSize
      .eval(commitContext(100), debugEnabled = true) should
      be(ConditionCommitResult(commitTriggered = true, "fileSize*: '100/100'".some))
    fileSize
      .eval(commitContext(100), debugEnabled = false) should
      be(ConditionCommitResult(commitTriggered = true, none))
  }

  "fileSize" should "return true when fileSize exceeded" in {
    fileSize
      .eval(commitContext(101), debugEnabled = true) should
      be(ConditionCommitResult(commitTriggered = true, "fileSize*: '101/100'".some))
    fileSize
      .eval(commitContext(101), debugEnabled = false) should
      be(ConditionCommitResult(commitTriggered = true, none))
  }

}
