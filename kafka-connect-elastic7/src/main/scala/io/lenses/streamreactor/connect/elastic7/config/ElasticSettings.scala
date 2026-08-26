/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.elastic7.config

import io.lenses.streamreactor.common.util.EitherUtils.unpackOrThrow
import io.lenses.streamreactor.common.security.StoresInfo
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings

/**
 * Factory for [[ElasticCommonSettings]] using the elastic7 [[ElasticConfig]] / [[ElasticConfigConstants]].
 * The case class is a type alias so existing code referencing `ElasticSettings` continues to compile.
 */
object ElasticSettings {

  def apply(config: ElasticConfig): ElasticCommonSettings = {
    val kcql                  = config.getKcql()
    val pkJoinerSeparator     = config.getString(ElasticConfigConstants.PK_JOINER_SEPARATOR)
    val writeTimeout          = config.getWriteTimeout
    val errorPolicy           = config.getErrorPolicy
    val retries               = config.getNumberRetries
    val httpBasicAuthUsername = config.getString(ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_USERNAME)
    val httpBasicAuthPassword = config.getString(ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_PASSWORD)
    val batchSize             = config.getInt(ElasticConfigConstants.BATCH_SIZE_CONFIG)
    val strictItemErrors      = config.getBoolean(ElasticConfigConstants.BULK_STRICT_ITEM_ERRORS_KEY)

    ElasticCommonSettings(
      kcqls                 = kcql,
      errorPolicy           = errorPolicy,
      taskRetries           = retries,
      writeTimeout          = writeTimeout,
      batchSize             = batchSize,
      pkJoinerSeparator     = pkJoinerSeparator,
      httpBasicAuthUsername = httpBasicAuthUsername,
      httpBasicAuthPassword = httpBasicAuthPassword,
      storesInfo            = unpackOrThrow(StoresInfo.fromConfig(config)),
      strictItemErrors      = strictItemErrors,
    )
  }
}
