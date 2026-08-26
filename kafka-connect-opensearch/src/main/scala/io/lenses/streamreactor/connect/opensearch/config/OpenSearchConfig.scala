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
package io.lenses.streamreactor.connect.opensearch.config

import io.lenses.streamreactor.common.config.base.traits.BaseConfig
import io.lenses.streamreactor.common.config.base.traits.ErrorPolicySettings
import io.lenses.streamreactor.common.config.base.traits.NumberRetriesSettings
import io.lenses.streamreactor.common.config.base.traits.WriteTimeoutSettings
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigDef
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

import java.util

case class OpenSearchConfig(props: Map[String, String])
    extends BaseConfig(CONNECTOR_PREFIX, OpenSearchConfig.config, props)
    with WriteTimeoutSettings
    with ErrorPolicySettings
    with NumberRetriesSettings {
  // Override case-class default toString so PASSWORD-typed keys are masked by AbstractConfig.
  override def toString: String = s"OpenSearchConfig(${super[BaseConfig].toString})"
}

object OpenSearchConfig {

  def apply(props: util.Map[String, String]): OpenSearchConfig = {
    import scala.jdk.CollectionConverters.MapHasAsScala
    new OpenSearchConfig(props.asScala.toMap)
  }

  val config: ConfigDef = ElasticCommonConfigDef
    .builder(
      prefix             = CONNECTOR_PREFIX,
      portDefault        = 9200,
      includeClusterName = false,
    )
    // JWT auth keys
    .define(JWT_TOKEN_KEY, Type.PASSWORD, JWT_TOKEN_DEFAULT, Importance.LOW, JWT_TOKEN_DOC)
    .define(JWT_TOKEN_FILE_KEY, Type.STRING, JWT_TOKEN_FILE_DEFAULT, Importance.LOW, JWT_TOKEN_FILE_DOC)
    .define(JWT_REFRESH_INTERVAL_KEY, Type.LONG, JWT_REFRESH_INTERVAL_DEFAULT, Importance.LOW, JWT_REFRESH_INTERVAL_DOC)
    .define(JWT_TOKEN_BASE_DIR_KEY, Type.STRING, JWT_TOKEN_BASE_DIR_DEFAULT, Importance.MEDIUM, JWT_TOKEN_BASE_DIR_DOC)
    // AWS SigV4 keys
    .define(AWS_SIGNING_ENABLED_KEY,
            Type.BOOLEAN,
            AWS_SIGNING_ENABLED_DEFAULT,
            Importance.MEDIUM,
            AWS_SIGNING_ENABLED_DOC,
    )
    .define(AWS_REGION_KEY, Type.STRING, AWS_REGION_DEFAULT, Importance.MEDIUM, AWS_REGION_DOC)
    .define(AWS_SIGNING_SERVICE_KEY,
            Type.STRING,
            AWS_SIGNING_SERVICE_DEFAULT,
            Importance.MEDIUM,
            AWS_SIGNING_SERVICE_DOC,
    )
    .define(
      AWS_CREDENTIALS_PROVIDER_KEY,
      Type.STRING,
      AWS_CREDENTIALS_PROVIDER_DEFAULT,
      Importance.MEDIUM,
      AWS_CREDENTIALS_PROVIDER_DOC,
    )
    .define(AWS_ACCESS_KEY_ID_KEY, Type.STRING, AWS_ACCESS_KEY_ID_DEFAULT, Importance.MEDIUM, AWS_ACCESS_KEY_ID_DOC)
    .define(AWS_SECRET_ACCESS_KEY_KEY,
            Type.PASSWORD,
            AWS_SECRET_ACCESS_KEY_DEFAULT,
            Importance.MEDIUM,
            AWS_SECRET_ACCESS_KEY_DOC,
    )
    .define(AWS_SESSION_TOKEN_KEY, Type.PASSWORD, AWS_SESSION_TOKEN_DEFAULT, Importance.LOW, AWS_SESSION_TOKEN_DOC)
    // Strict bulk item errors
    .define(BULK_STRICT_ITEM_ERRORS_KEY,
            Type.BOOLEAN,
            BULK_STRICT_ITEM_ERRORS_DEFAULT,
            Importance.MEDIUM,
            BULK_STRICT_ITEM_ERRORS_DOC,
    )
    // Connection pool tuning (REST path only)
    .define(MAX_CONNECTIONS_PER_ROUTE_KEY,
            Type.INT,
            MAX_CONNECTIONS_PER_ROUTE_DEFAULT,
            Importance.LOW,
            MAX_CONNECTIONS_PER_ROUTE_DOC,
    )
    .define(MAX_CONNECTIONS_TOTAL_KEY,
            Type.INT,
            MAX_CONNECTIONS_TOTAL_DEFAULT,
            Importance.LOW,
            MAX_CONNECTIONS_TOTAL_DOC,
    )
    // Security: opt-out from cleartext-auth protection
    .define(ALLOW_INSECURE_AUTH_KEY,
            Type.BOOLEAN,
            ALLOW_INSECURE_AUTH_DEFAULT,
            Importance.MEDIUM,
            ALLOW_INSECURE_AUTH_DOC,
    )
}
