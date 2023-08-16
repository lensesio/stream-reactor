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

import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FileSizeTest extends AnyFlatSpec with Matchers with EitherValues with MockitoSugar {

  private val fileSize = FileSize(100)

  private def commitContext(currentFileSize: Long): CommitContext = {
    val cc = mock[CommitContext]
    when(cc.fileSize).thenReturn(currentFileSize)
    when(cc.connectorTaskId).thenReturn(ConnectorTaskId("connector", 1, 0))
    cc
  }

  "fileSize" should "return false when fileSize not reached yet" in {
    fileSize
      .eval(commitContext(99)) should
      be(ConditionCommitResult(false, "File Size Policy: 99/100."))
  }

  "fileSize" should "return true when fileSize reached" in {
    fileSize
      .eval(commitContext(100)) should
      be(ConditionCommitResult(true, "File Size Policy: 100/100."))

  }

  "fileSize" should "return true when fileSize exceeded" in {
    fileSize
      .eval(commitContext(101)) should
      be(ConditionCommitResult(true, "File Size Policy: 101/100."))
  }

}
