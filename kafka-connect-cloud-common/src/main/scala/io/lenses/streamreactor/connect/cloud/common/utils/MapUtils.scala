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
package io.lenses.streamreactor.connect.cloud.common.utils

object MapUtils {

  /**
    * Kafka Connect provides properties both in the SinkTask.start as
    * well as enables reading the properties via the Connector contextProps.
    * This is a simple utility to merge the properties from both sources.
    *
    * This is done because the context properties can contain newer values for items like secret
    * Get the Task configuration. This is the latest configuration and may differ from that passed on startup. For example, this method can be used to obtain the latest configuration if an external secret has changed, and the configuration is using variable references such as those compatible with org.apache.kafka.common.config.ConfigTransformer.
    *
    * @param context  connector properties from the contextProps
    * @param properties connector properties from the start method
    * @return Map[String,String] properties from both sources merged
    */
  def mergeProps(
    context:    Map[String, String],
    properties: Map[String, String],
  ): Map[String, String] = properties ++ context

}
