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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IndexManagerErrorsTest extends AnyFlatSpec with Matchers {

  "corruptStorageState" should "return the correct error message" in {
    val system = "TestSystem"
    val expectedError =
      """
        |The TestSystem storage state is corrupted. The connector state is out of sync
        |with the data. This could happen if the connector has been recreated and the data was deleted.
        |Delete the connector's .index subfolder as well and restart the connector.""".stripMargin

    val result = IndexManagerErrors.corruptStorageState(system)
    result shouldBe expectedError
  }

  "fileDeleteError" should "return the correct error message" in {
    val system = "TestSystem"
    val expectedError =
      """
        |There was an issue deleting old index files from the indexes directory.  This could happen if
        |you have not granted the connector role appropriate delete permissions via the TestSystem
        |permissions model.""".stripMargin

    val result = IndexManagerErrors.fileDeleteError(system)
    result shouldBe expectedError
  }
}
