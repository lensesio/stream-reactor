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
package io.lenses.streamreactor.connect.gcp.storage.source.config

import org.apache.kafka.common.config.ConfigException
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._


class GCPStorageSourceConfigTest extends AnyFunSuite with EitherValues {

  test("fromProps should reject configuration when no kcql string is provided") {
    val props  = Map[String, String]()
    val result = GCPStorageSourceConfig.fromProps(props)

    assertEitherException(
      result,
      classOf[ConfigException].getName,
      "Missing required configuration \"connect.gcpstorage.kcql\" which has no default value.",
    )
  }

  test("fromProps should reject configuration when kcql doesn't parse") {
    val props = Map[String, String](
      "connect.gcpstorage.kcql" -> "flibble dibble dop",
    )
    val result = GCPStorageSourceConfig.fromProps(props)
    assertEitherException(
      result,
      classOf[IllegalArgumentException].getName,
      "Invalid syntax.failed to parse at line 1 due to mismatched input 'flibble' expecting {INSERT, UPSERT, UPDATE, SELECT}",
    )
  }

  test("fromProps should reject configuration when invalid bucket name is provided") {
    val props = Map[String, String](
      "connect.gcpstorage.kcql" -> "select * from myBucket insert into myTopic",
    )
    val result = GCPStorageSourceConfig.fromProps(props)
    assertEitherException(result,
                          classOf[IllegalArgumentException].getName,
                          "Invalid bucket name (Rule: Bucket name should match regex",
    )
  }

  test("fromProps should reject configuration when invalid auth mode is provided") {
    val props = Map[String, String](
      "connect.gcpstorage.kcql"          -> "select * from myBucket.azure insert into myTopic",
      "connect.gcpstorage.gcp.auth.mode" -> "plain-and-unencrypted",
    )
    val result = GCPStorageSourceConfig.fromProps(props)
    assertEitherException(result, classOf[ConfigException].getName, "Unsupported auth mode `plain-and-unencrypted`")
  }

  test("apply should return Right with GCPStorageSourceConfig when valid properties are provided") {
    val props = Map[String, String](
      "connect.gcpstorage.kcql"          -> "select * from myBucket.azure insert into myTopic",
      "connect.gcpstorage.gcp.auth.mode" -> "credentials",
    )
    val result = GCPStorageSourceConfig(GCPStorageSourceConfigDefBuilder(props))
    result.isRight shouldBe true
  }

  private def assertEitherException(
    result:                 Either[Throwable, GCPStorageSourceConfig],
    expectedExceptionClass: String,
    expectedMessage:        String,
  ): Any =
    result.left.value match {
      case ex if expectedExceptionClass == ex.getClass.getName =>
        ex.getMessage should be(expectedMessage)
      case ex => fail(s"Unexpected exception, was a ${ex.getClass.getName}")
    }
}
