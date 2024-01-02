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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.LocalStagingArea
import io.lenses.streamreactor.connect.cloud.common.sink.config.LocalStagingAreaConfigKeys
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

class LocalStagingAreaTest extends AnyFlatSpec with Matchers with EitherValues with LocalStagingAreaConfigKeys {
  implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  private val tmpDir = Files.createTempDirectory("S3OutputStreamOptionsTest")

  behavior of "LocalStagingArea"

  it should "create BuildLocalOutputStreamOptions when temp directory has been supplied" in {
    implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("unusedSinkName", 1, 1)
    TestConfig(LOCAL_TMP_DIRECTORY -> s"$tmpDir/my/path").getLocalStagingArea() should
      be(Right(LocalStagingArea(new File(s"$tmpDir/my/path"))))
  }

  it should "create BuildLocalOutputStreamOptions when temp directory and sink name has been supplied" in {
    implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("unusedSinkName", 1, 1)
    // should ignore the sinkName
    TestConfig(LOCAL_TMP_DIRECTORY -> s"$tmpDir/my/path").getLocalStagingArea() should
      be(Right(LocalStagingArea(new File(s"$tmpDir/my/path"))))
  }

  it should "create BuildLocalOutputStreamOptions when only sink name has been supplied" in {
    implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("superSleekSinkName", 1, 1)
    val tempDir = System.getProperty("java.io.tmpdir")
    val result  = TestConfig().getLocalStagingArea()
    result.isRight should be(true)
    result.value match {
      case LocalStagingArea(file) => file.toString should startWith(s"$tempDir/superSleekSinkName".replace("//", "/"))
      case _                      => fail("Wrong")
    }
  }

  override def connectorPrefix: String = "connect.testing"
}
