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
package io.lenses.streamreactor.connect.aws.s3.sink.config

import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.CONNECTOR_PREFIX
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.cloud.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.sink.config.LocalStagingArea
import io.lenses.streamreactor.connect.cloud.sink.config.LocalStagingAreaConfigKeys
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

class LocalStagingAreaTest extends AnyFlatSpec with Matchers with EitherValues with LocalStagingAreaConfigKeys {
  implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator

  private val tmpDir = Files.createTempDirectory("S3OutputStreamOptionsTest")
  behavior of "LocalStagingArea"

  it should "create BuildLocalOutputStreamOptions when temp directory has been supplied" in {
    implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("unusedSinkName", 1, 1)
    TestConfigDefBuilder(LOCAL_TMP_DIRECTORY -> s"$tmpDir/my/path").getLocalStagingArea() should
      be(Right(LocalStagingArea(new File(s"$tmpDir/my/path"))))
  }

  it should "create BuildLocalOutputStreamOptions when temp directory and sink name has been supplied" in {
    implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("unusedSinkName", 1, 1)
    // should ignore the sinkName
    TestConfigDefBuilder(LOCAL_TMP_DIRECTORY -> s"$tmpDir/my/path").getLocalStagingArea() should
      be(Right(LocalStagingArea(new File(s"$tmpDir/my/path"))))
  }

  it should "create BuildLocalOutputStreamOptions when only sink name has been supplied" in {
    implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("superSleekSinkName", 1, 1)
    val tempDir = System.getProperty("java.io.tmpdir")
    val result  = TestConfigDefBuilder().getLocalStagingArea()
    result.isRight should be(true)
    result.value match {
      case LocalStagingArea(file) => file.toString should startWith(s"$tempDir/superSleekSinkName".replace("//", "/"))
      case _                      => fail("Wrong")
    }
  }

  override def connectorPrefix: String = CONNECTOR_PREFIX
}
