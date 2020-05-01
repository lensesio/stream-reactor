/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.elastic6.config

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicy

/**
  * Created by andrew@datamountaineer.com on 13/05/16.
  * stream-reactor-maven
  */
case class ElasticSettings(kcqls: Seq[Kcql],
                           errorPolicy: ErrorPolicy,
                           taskRetries: Int = ElasticConfigConstants.NBR_OF_RETIRES_DEFAULT,
                           writeTimeout: Int = ElasticConfigConstants.WRITE_TIMEOUT_DEFAULT,
                           batchSize: Int = ElasticConfigConstants.BATCH_SIZE_DEFAULT,
                           pkJoinerSeparator: String = ElasticConfigConstants.PK_JOINER_SEPARATOR_DEFAULT,
                           httpBasicAuthUsername: String = ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_USERNAME_DEFAULT,
                           httpBasicAuthPassword: String = ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_USERNAME_DEFAULT
                          )


object ElasticSettings {

  def apply(config: ElasticConfig): ElasticSettings = {
    val kcql = config.getKcql()
    val pkJoinerSeparator = config.getString(ElasticConfigConstants.PK_JOINER_SEPARATOR)
    val writeTimeout = config.getWriteTimeout
    val errorPolicy = config.getErrorPolicy
    val retries = config.getNumberRetries
    val httpBasicAuthUsername = config.getString(ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_USERNAME)
    val httpBasicAuthPassword = config.getString(ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_PASSWORD)

    val batchSize = config.getInt(ElasticConfigConstants.BATCH_SIZE_CONFIG)

    ElasticSettings(kcql,
      errorPolicy,
      retries,
      writeTimeout,
      batchSize,
      pkJoinerSeparator,
      httpBasicAuthUsername,
      httpBasicAuthPassword
    )
  }
}
