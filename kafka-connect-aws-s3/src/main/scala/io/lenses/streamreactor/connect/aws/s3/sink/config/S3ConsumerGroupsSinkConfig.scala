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

import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.config._
import io.lenses.streamreactor.connect.cloud.common.config.PropertiesHelper
import io.lenses.streamreactor.connect.cloud.common.consumers.CloudObjectKey

case class S3ConsumerGroupsSinkConfig(
  location: CloudObjectKey,
  config:   S3ConnectionConfig,
)

object S3ConsumerGroupsSinkConfig extends PropertiesHelper {
  def fromProps(
    props: Map[String, String],
  ): Either[Throwable, S3ConsumerGroupsSinkConfig] =
    S3ConsumerGroupsSinkConfig(S3ConsumerGroupsSinkConfigDef(props))

  def apply(
    s3ConfigDefBuilder: S3ConsumerGroupsSinkConfigDef,
  ): Either[Throwable, S3ConsumerGroupsSinkConfig] =
    S3ConsumerGroupsSinkConfig.from(
      s3ConfigDefBuilder.getParsedValues,
    )

  def from(props: Map[String, _]): Either[Throwable, S3ConsumerGroupsSinkConfig] =
    for {
      bucketAndPrefix <- getStringEither(props, S3_BUCKET_CONFIG)
      bucket          <- CloudObjectKey.from(bucketAndPrefix)
      _               <- AuthMode.withNameInsensitiveEither(getString(props, AUTH_MODE).getOrElse(AuthMode.Default.toString))
    } yield {
      S3ConsumerGroupsSinkConfig(
        bucket,
        S3ConnectionConfig(props),
      )
    }
}
