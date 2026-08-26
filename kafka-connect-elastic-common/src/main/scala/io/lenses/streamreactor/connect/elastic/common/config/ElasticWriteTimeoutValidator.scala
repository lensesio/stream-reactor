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

import org.apache.kafka.common.config.ConfigException

object ElasticWriteTimeoutValidator {

  def validate(millis: Int, configKey: String): Unit =
    if (
      millis >= ElasticCommonConfigConstants.WRITE_TIMEOUT_SECONDS_TRAP_MIN &&
      millis <= ElasticCommonConfigConstants.WRITE_TIMEOUT_SECONDS_TRAP_MAX
    ) {
      throw new ConfigException(
        s"$configKey=$millis looks like seconds from the pre-migration unit; use milliseconds (e.g. 60000 for one minute)",
      )
    }
}
