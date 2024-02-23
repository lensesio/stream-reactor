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
package io.lenses.streamreactor.connect.cloud.common.config.traits

import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.connect.cloud.common.config.RetryConfig

/**
  * Trait representing configuration for a cloud connection.
  * This trait defines methods for retrieving error policy and connector retry configuration.
  */
trait CloudConnectionConfig {

  /**
    * Retrieves the error policy for the cloud connection.
    *
    * @return The error policy for the cloud connection.
    */
  def errorPolicy: ErrorPolicy

  /**
    * Retrieves the retry configuration for the cloud connection.
    *
    * @return The retry configuration for the cloud connection.
    */
  def connectorRetryConfig: RetryConfig
}
