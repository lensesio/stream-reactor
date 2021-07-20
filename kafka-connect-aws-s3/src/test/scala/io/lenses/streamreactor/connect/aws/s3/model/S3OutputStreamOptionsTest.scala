/*
 * Copyright 2021 Lenses.io
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

import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigDefBuilder
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.LOCAL_TMP_DIRECTORY
import io.lenses.streamreactor.connect.aws.s3.model.BuildLocalOutputStreamOptions.PROPERTY_SINK_NAME
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.jdk.CollectionConverters.mapAsJavaMapConverter

class S3OutputStreamOptionsTest extends AnyFlatSpec with Matchers {

  private val tmpDir = Files.createTempDirectory("S3OutputStreamOptionsTest")

  behavior of "S3OutputStreamOptions"

  it should "create StreamedOutputStreamOptions with warning when no write mode supplied" in {
    S3OutputStreamOptions("", adapt(Map.empty[String, String])) should
      be (Right(StreamedWriteOutputStreamOptions()))
  }

  it should "create StreamedOutputStreamOptions with warning when invalid write mode supplied" in {
    S3OutputStreamOptions("whatisit", adapt(Map.empty[String, String])) should
      be (Right(StreamedWriteOutputStreamOptions()))
  }

  private def adapt(map : Map[String,String], sinkName : Option[String] = None) = {
    val newMap = map + {"connect.s3.kcql" -> "assda"}
    S3ConfigDefBuilder(sinkName, newMap.asJava)
  }

  it should "create StreamedOutputStreamOptions when invalid write mode supplied" in {
    S3OutputStreamOptions("Streamed",  adapt(Map.empty[String, String])) should
      be (Right(StreamedWriteOutputStreamOptions()))
  }

  it should "create BuildLocalOutputStreamOptions when temp directory has been supplied" in {
    S3OutputStreamOptions("buildlocal", adapt(Map(LOCAL_TMP_DIRECTORY -> s"$tmpDir/my/path"))) should
      be (Right(BuildLocalOutputStreamOptions(LocalLocation(s"$tmpDir/my/path"))))
  }

  it should "create BuildLocalOutputStreamOptions when temp directory and sink name has been supplied" in {
    // should ignore the sinkName
    S3OutputStreamOptions("BuildLocal", adapt(Map(LOCAL_TMP_DIRECTORY -> s"$tmpDir/my/path", PROPERTY_SINK_NAME -> "superSleekSinkName"))) should
      be (Right(BuildLocalOutputStreamOptions(LocalLocation(s"$tmpDir/my/path"))))
  }

  it should "create BuildLocalOutputStreamOptions when sink name has been supplied" in {
    val tempDir = System.getProperty("java.io.tmpdir")
    val result = S3OutputStreamOptions("BUILDLOCAL", adapt(Map(), Option("superSleekSinkName")))
    result.isRight should be (true)
    result.right.get match {
      case BuildLocalOutputStreamOptions(localLocation) => localLocation.path should startWith (s"$tempDir/superSleekSinkName")
      case _ => fail("Wrong")
    }
  }

  it should "not create BuildLocalOutputStreamOptions when nothing supplied" in {
    val result = S3OutputStreamOptions("bUiLdLoCal", adapt(Map[String,String]()))
    result.isLeft should be (true)
    result.left.get.getClass.getSimpleName should be ("IllegalStateException")
    result.left.get.getMessage should be ("Either a local temporary directory (connect.s3.local.tmp.directory) or a Sink Name (name) must be configured to use 'BuildLocal' write mode.")
  }

}
