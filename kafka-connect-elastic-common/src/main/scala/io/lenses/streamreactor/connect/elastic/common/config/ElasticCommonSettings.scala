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
package io.lenses.streamreactor.connect.elastic.common.config

import cyclops.control.{ Option => COption }
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.common.security.StoresInfo

/**
 * Shared settings for all elastic/opensearch connectors.
 */
case class ElasticCommonSettings(
  kcqls:                 Seq[Kcql],
  errorPolicy:           ErrorPolicy,
  taskRetries:           Int        = ElasticCommonConfigConstants.NBR_OF_RETIRES_DEFAULT,
  writeTimeout:          Int        = ElasticCommonConfigConstants.WRITE_TIMEOUT_DEFAULT,
  batchSize:             Int        = ElasticCommonConfigConstants.BATCH_SIZE_DEFAULT,
  pkJoinerSeparator:     String     = ElasticCommonConfigConstants.PK_JOINER_SEPARATOR_DEFAULT,
  httpBasicAuthUsername: String     = ElasticCommonConfigConstants.CLIENT_HTTP_BASIC_AUTH_USERNAME_DEFAULT,
  httpBasicAuthPassword: String     = ElasticCommonConfigConstants.CLIENT_HTTP_BASIC_AUTH_PASSWORD_DEFAULT,
  storesInfo:            StoresInfo = new StoresInfo(COption.none(), COption.none(), COption.none()),
  strictItemErrors:      Boolean    = ElasticCommonConfigConstants.BULK_STRICT_ITEM_ERRORS_DEFAULT,
)
