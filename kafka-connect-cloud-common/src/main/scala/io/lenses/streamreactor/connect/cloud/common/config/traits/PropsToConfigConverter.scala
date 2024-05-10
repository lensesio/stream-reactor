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

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator

/**
  * Trait for converting properties to a specific cloud configuration.
  * Implementations of this trait are responsible for parsing properties
  * and constructing a cloud configuration object of type C.
  */
trait PropsToConfigConverter[C <: CloudConfig] {

  /**
    * Converts a map of properties to a cloud configuration object of type C.
    *
    * @param connectorTaskId        The identifier for the connector task.
    *                               This is used to associate the configuration with a specific task.
    * @param props                  The properties to be converted.
    *                               These properties contain the configuration settings.
    * @param cloudLocationValidator An implicit validator for cloud locations.
    *                               This validator is used to ensure that cloud location information
    *                               specified in the properties is valid.
    * @return                       Either a Throwable if an error occurs during conversion,
    *                               or a cloud configuration object of type C.
    *                               The left side of the Either contains an error if the conversion fails,
    *                               otherwise, the right side contains the successfully converted configuration.
    */
  def fromProps(
    connectorTaskId: ConnectorTaskId,
    props:           Map[String, AnyRef],
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, C]

}
