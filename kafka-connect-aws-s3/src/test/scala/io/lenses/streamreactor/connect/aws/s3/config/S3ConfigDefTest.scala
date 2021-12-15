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

package io.lenses.streamreactor.connect.aws.s3.config

import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class S3ConfigDefTest extends AnyFlatSpec with Matchers {

  private val DeprecatedProps: Map[String, String] = Map(
    DEP_AWS_ACCESS_KEY -> "DepAccessKey",
    DEP_AWS_SECRET_KEY -> "DepSecretKey",
    DEP_AUTH_MODE -> AuthMode.Credentials.toString,
    DEP_CUSTOM_ENDPOINT -> "http://deprecated",
    DEP_ENABLE_VIRTUAL_HOST_BUCKETS -> "true",
    KCQL_CONFIG -> "SELECT * FROM DEPRECATED",
  )

  private val DefaultProps: Map[String, String] = Map(
    AWS_ACCESS_KEY -> "AccessKey",
    AWS_SECRET_KEY -> "SecretKey",
    AUTH_MODE -> AuthMode.Default.toString,
    CUSTOM_ENDPOINT -> "http://default",
    ENABLE_VIRTUAL_HOST_BUCKETS -> "true",
    KCQL_CONFIG -> "SELECT * FROM DEFAULT",
  )

  "S3ConfigDef" should "parse original properties" in {
    val resultMap = S3ConfigDef.config.parse(DefaultProps.asJava).asScala
    resultMap should have size (14)
    DeprecatedProps.filterNot { case (k, _) => k == KCQL_CONFIG }.foreach { case (k, _) => resultMap.get(k) should be(None) }
    DefaultProps.foreach { case (k, _) => resultMap.keySet.contains(k) should be(true) }
  }

  "S3ConfigDef" should "parse deprecated properties" in {
    val resultMap = S3ConfigDef.config.parse(DeprecatedProps.asJava).asScala
    resultMap should have size (14)
    DeprecatedProps.filterNot { case (k, _) => k == KCQL_CONFIG }.foreach { case (k, _) => resultMap.get(k) should be(None) }
    DefaultProps.foreach { case (k, _) => resultMap.keySet.contains(k) should be(true) }
  }

  "S3ConfigDef" should "parse merged properties" in {
    val mergedProps = DefaultProps.combine(DeprecatedProps)
    val resultMap = S3ConfigDef.config.parse(mergedProps.asJava).asScala
    DeprecatedProps.filterNot { case (k, _) => k == KCQL_CONFIG }.foreach { case (k, _) => resultMap.get(k) should be(None) }
    DefaultProps.foreach { case (k, _) => resultMap.keySet.contains(k) should be(true) }
  }
}
