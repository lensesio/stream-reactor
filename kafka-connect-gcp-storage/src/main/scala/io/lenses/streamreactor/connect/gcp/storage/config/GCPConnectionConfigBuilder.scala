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
package io.lenses.streamreactor.connect.gcp.storage.config

import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse._
import io.lenses.streamreactor.connect.gcp.common.auth.mode.AuthMode
import io.lenses.streamreactor.connect.gcp.common.auth.GCPConnectionConfig
import io.lenses.streamreactor.connect.gcp.common.auth.HttpTimeoutConfig
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings._

import scala.jdk.OptionConverters.RichOption

object GCPConnectionConfigBuilder {

  def apply(props: Map[String, _], authMode: AuthMode): GCPConnectionConfig = new GCPConnectionConfig(
    getString(props, GCP_PROJECT_ID).toJava,
    getString(props, GCP_QUOTA_PROJECT_ID).toJava,
    authMode,
    getString(props, HOST).toJava,
    new RetryConfig(
      getInt(props, HTTP_NBR_OF_RETRIES).getOrElse(HTTP_NBR_OF_RETIRES_DEFAULT),
      getLong(props, HTTP_ERROR_RETRY_INTERVAL).getOrElse(HTTP_ERROR_RETRY_INTERVAL_DEFAULT),
    ),
    new HttpTimeoutConfig(
      getLong(props, HTTP_SOCKET_TIMEOUT).map(Long.box).toJava,
      getLong(props, HTTP_CONNECTION_TIMEOUT).map(Long.box).toJava,
    ),
  )

}
