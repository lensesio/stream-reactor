/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.redis.sink.writer

import io.lenses.kcql.Kcql
import com.typesafe.scalalogging.StrictLogging

import scala.jdk.CollectionConverters.MapHasAsScala

trait GeoAddSupport extends StrictLogging {

  def getLongitudeField(kcqlConfig: Kcql): String =
    getStoredAsParameter("longitudeField", kcqlConfig, "longitude")

  def getLatitudeField(kcqlConfig: Kcql): String =
    getStoredAsParameter("latitudeField", kcqlConfig, "latitude")

  def getStoredAsParameter(parameterName: String, kcqlConfig: Kcql, defaultValue: String): String = {
    val geoAddParams = kcqlConfig.getStoredAsParameters.asScala
    val parameterValue =
      if (geoAddParams.keys.exists(k => k.equalsIgnoreCase(parameterName)))
        geoAddParams.find { case (k, _) => k.equalsIgnoreCase(parameterName) }.get._2
      else {
        logger.info(s"You have not defined a [$parameterName] field. We'll try to fall back to [$defaultValue] field")
        defaultValue
      }
    parameterValue
  }
}
