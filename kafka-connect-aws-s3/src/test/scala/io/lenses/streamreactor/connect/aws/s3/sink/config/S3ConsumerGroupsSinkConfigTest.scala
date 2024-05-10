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
package io.lenses.streamreactor.connect.aws.s3.sink.config

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.HttpTimeoutConfig
import io.lenses.streamreactor.connect.aws.s3.config.S3ConnectionConfig
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.cloud.common.consumers.CloudObjectKey
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class S3ConsumerGroupsSinkConfigTest extends AnyFunSuite with Matchers {
  test("creates an instance of S3ConsumerGroupsSinkConfig") {
    S3ConsumerGroupsSinkConfig.fromProps(
      Map(
        S3_BUCKET_CONFIG -> "bucket:a/b/c",
        AWS_REGION       -> "eu-west-1",
        AWS_ACCESS_KEY   -> "access",
        AWS_SECRET_KEY   -> "secret",
        AUTH_MODE        -> "credentials",
        CUSTOM_ENDPOINT  -> "endpoint",
      ),
    ) match {
      case Left(value) => fail("Expecting to build a config but got an error instead.", value)
      case Right(value) =>
        value should be(
          S3ConsumerGroupsSinkConfig(
            CloudObjectKey("bucket", "a/b/c".some),
            S3ConnectionConfig(
              Some("eu-west-1"),
              Some("access"),
              Some("secret"),
              AuthMode.Credentials,
              Some("endpoint"),
              false,
              new RetryConfig(5, 50),
              HttpTimeoutConfig(Some(60000), Some(60000)),
              None,
            ),
          ),
        )
    }
  }

  test("remove the / from the prefix") {
    S3ConsumerGroupsSinkConfig.fromProps(
      Map(
        S3_BUCKET_CONFIG -> "bucket:a/b/c/",
        AWS_REGION       -> "eu-west-1",
        AWS_ACCESS_KEY   -> "access",
        AWS_SECRET_KEY   -> "secret",
        AUTH_MODE        -> "credentials",
        CUSTOM_ENDPOINT  -> "endpoint",
      ),
    ) match {
      case Left(value) => fail("Expecting to build a config but got an error instead.", value)
      case Right(value) =>
        value should be(
          S3ConsumerGroupsSinkConfig(
            CloudObjectKey("bucket", "a/b/c".some),
            new S3ConnectionConfig(
              Some("eu-west-1"),
              Some("access"),
              Some("secret"),
              AuthMode.Credentials,
              Some("endpoint"),
              false,
              new RetryConfig(5, 50),
              HttpTimeoutConfig(Some(60000), Some(60000)),
              None,
            ),
          ),
        )
    }
  }
}
