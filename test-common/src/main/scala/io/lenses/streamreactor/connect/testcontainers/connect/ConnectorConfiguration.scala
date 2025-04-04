/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.testcontainers.connect

import org.json4s.DefaultFormats
import org.json4s.native.Serialization

case class ConnectorConfiguration(
  name:   String,
  config: Map[String, ConfigValue[_]],
) {

  implicit val formats: DefaultFormats.type = DefaultFormats

  def toJson(): String = {
    val mergedConfigMap = config + ("tasks.max" -> ConfigValue(1))
    Serialization.write(
      Map[String, Any](
        "name"   -> name,
        "config" -> transformConfigMap(mergedConfigMap),
      ),
    )
  }

  private def transformConfigMap(
    originalMap: Map[String, ConfigValue[_]],
  ): Map[String, Any] = originalMap.view.mapValues(_.underlying).toMap

}
