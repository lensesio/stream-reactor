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

import com.datamountaineer.streamreactor.common.config.base.traits._
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.config._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

import java.util
import scala.jdk.CollectionConverters._

object S3ConsumerGroupsSinkConfigDef {

  val config: ConfigDef = new ConfigDef()
    .define(S3_BUCKET_CONFIG,
            Type.STRING,
            Importance.HIGH,
            S3_BUCKET_DOC,
            "S3",
            1,
            ConfigDef.Width.LONG,
            S3_BUCKET_CONFIG,
    )
    .define(
      AWS_REGION,
      Type.STRING,
      "",
      Importance.HIGH,
      "AWS region",
    )
    .define(
      AWS_ACCESS_KEY,
      Type.PASSWORD,
      "",
      Importance.HIGH,
      "AWS access key",
    )
    .define(
      AWS_SECRET_KEY,
      Type.PASSWORD,
      "",
      Importance.HIGH,
      "AWS password key",
    )
    .define(
      AUTH_MODE,
      Type.STRING,
      AuthMode.Default.toString,
      Importance.HIGH,
      "Authenticate mode, 'credentials' or 'default'",
    )
    .define(
      CUSTOM_ENDPOINT,
      Type.STRING,
      "",
      Importance.LOW,
      "Custom S3-compatible endpoint (usually for testing)",
    )
    .define(
      ENABLE_VIRTUAL_HOST_BUCKETS,
      Type.BOOLEAN,
      false,
      Importance.LOW,
      "Enable virtual host buckets",
    ).define(
      HTTP_NBR_OF_RETRIES,
      Type.INT,
      HTTP_NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      HTTP_NBR_OF_RETRIES_DOC,
      "Error",
      2,
      ConfigDef.Width.LONG,
      HTTP_NBR_OF_RETRIES,
    )
    .define(
      HTTP_ERROR_RETRY_INTERVAL,
      Type.LONG,
      HTTP_ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      HTTP_ERROR_RETRY_INTERVAL_DOC,
      "Error",
      3,
      ConfigDef.Width.LONG,
      HTTP_ERROR_RETRY_INTERVAL,
    )
    .define(HTTP_SOCKET_TIMEOUT, Type.LONG, HTTP_SOCKET_TIMEOUT_DEFAULT, Importance.LOW, HTTP_SOCKET_TIMEOUT_DOC)
    .define(HTTP_CONNECTION_TIMEOUT,
            Type.INT,
            HTTP_CONNECTION_TIMEOUT_DEFAULT,
            Importance.LOW,
            HTTP_CONNECTION_TIMEOUT_DOC,
    )
    .define(
      POOL_MAX_CONNECTIONS,
      Type.INT,
      POOL_MAX_CONNECTIONS_DEFAULT,
      Importance.LOW,
      POOL_MAX_CONNECTIONS_DOC,
    )
}

case class S3ConsumerGroupsSinkConfigDef(props: util.Map[String, String])
    extends BaseConfig(S3ConfigSettings.CONNECTOR_PREFIX, S3ConsumerGroupsSinkConfigDef.config, props) {
  def getParsedValues: Map[String, _] = values().asScala.toMap

}
