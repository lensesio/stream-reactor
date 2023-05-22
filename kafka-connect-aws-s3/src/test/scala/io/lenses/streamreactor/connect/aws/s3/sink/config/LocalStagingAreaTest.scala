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

import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.LOCAL_TMP_DIRECTORY
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import scala.jdk.CollectionConverters.MapHasAsJava

class LocalStagingAreaTest extends AnyFlatSpec with Matchers with EitherValues {

  private val tmpDir = Files.createTempDirectory("S3OutputStreamOptionsTest")
  behavior of "LocalStagingArea"

  private def adapt(map: Map[String, String]) = {
    val newMap = map + {
      "connect.s3.kcql" -> "assda"
    }
    S3SinkConfigDefBuilder(newMap.asJava)
  }

  it should "create BuildLocalOutputStreamOptions when temp directory has been supplied" in {
    implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("unusedSinkName", 1, 1)
    LocalStagingArea(adapt(Map(LOCAL_TMP_DIRECTORY -> s"$tmpDir/my/path"))) should
      be(Right(LocalStagingArea(new File(s"$tmpDir/my/path"))))
  }

  it should "create BuildLocalOutputStreamOptions when temp directory and sink name has been supplied" in {
    implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("unusedSinkName", 1, 1)
    // should ignore the sinkName
    LocalStagingArea(adapt(Map(LOCAL_TMP_DIRECTORY -> s"$tmpDir/my/path"))) should
      be(Right(LocalStagingArea(new File(s"$tmpDir/my/path"))))
  }

  it should "create BuildLocalOutputStreamOptions when only sink name has been supplied" in {
    implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("superSleekSinkName", 1, 1)
    val tempDir = System.getProperty("java.io.tmpdir")
    val result  = LocalStagingArea(adapt(Map()))
    result.isRight should be(true)
    result.value match {
      case LocalStagingArea(file) => file.toString should startWith(s"$tempDir/superSleekSinkName".replace("//", "/"))
      case _                      => fail("Wrong")
    }
  }

  it should "not create BuildLocalOutputStreamOptions when nothing supplied" in {
    implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("sinkName", 1, 1)
    val result = LocalStagingArea(adapt(Map[String, String]()))
    result.left.value.getClass.getSimpleName should be("IllegalStateException")
    result.left.value.getMessage should be(
      "Either a local temporary directory (connect.s3.local.tmp.directory) or a Sink Name (name) must be configured.",
    )
  }

}
