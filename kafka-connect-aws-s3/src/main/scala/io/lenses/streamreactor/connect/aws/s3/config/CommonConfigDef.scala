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
package io.lenses.streamreactor.connect.aws.s3.config

import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.source.config.S3SourceConfigDef
import io.lenses.streamreactor.connect.cloud.common.config.CompressionCodecConfigKeys
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

trait CommonConfigDef extends CompressionCodecConfigKeys with DeleteModeConfigKeys {

  def config: ConfigDef = new S3SourceConfigDef()
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
    )
    .define(KCQL_CONFIG, Type.STRING, Importance.HIGH, KCQL_DOC)
    .define(
      ERROR_POLICY,
      Type.STRING,
      ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      ERROR_POLICY_DOC,
      "Error",
      1,
      ConfigDef.Width.LONG,
      ERROR_POLICY,
    )
    .define(
      NBR_OF_RETRIES,
      Type.INT,
      NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      NBR_OF_RETRIES_DOC,
      "Error",
      2,
      ConfigDef.Width.LONG,
      NBR_OF_RETRIES,
    )
    .define(
      ERROR_RETRY_INTERVAL,
      Type.LONG,
      ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      ERROR_RETRY_INTERVAL_DOC,
      "Error",
      3,
      ConfigDef.Width.LONG,
      ERROR_RETRY_INTERVAL,
    )
    .define(
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
    .define(
      COMPRESSION_CODEC,
      Type.STRING,
      COMPRESSION_CODEC_DEFAULT,
      Importance.LOW,
      COMPRESSION_CODEC_DOC,
    )
    .define(
      COMPRESSION_LEVEL,
      Type.INT,
      COMPRESSION_LEVEL_DEFAULT,
      Importance.LOW,
      COMPRESSION_LEVEL_DOC,
    )
    .define(
      DELETE_MODE,
      Type.STRING,
      DELETE_MODE_DEFAULT,
      Importance.LOW,
      DELETE_MODE_DOC,
    )
}
