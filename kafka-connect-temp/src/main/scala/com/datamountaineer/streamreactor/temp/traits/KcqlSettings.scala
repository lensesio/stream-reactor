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

package com.datamountaineer.streamreactor.temp.traits

import com.datamountaineer.connector.config.{Config, FormatType}
import com.datamountaineer.streamreactor.temp.const.TraitConfigConst.KCQL_PROP_SUFFIX

import scala.collection.JavaConversions._

trait KcqlSettings extends BaseSettings {
  val kcqlConstant: String = s"$connectorPrefix.$KCQL_PROP_SUFFIX"

  def getRoutes: Set[Config] = {
    val raw = getString(kcqlConstant)
    require(!raw.isEmpty, s"$kcqlConstant is empty.")
    raw.split(";").map(r => Config.parse(r)).toSet
  }

  def getFields(routes: Set[Config] = getRoutes): Map[String, Map[String, String]] = {
    routes.map(rm =>
      (rm.getSource, rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)
    ).toMap
  }

  def getIgnoreFields(routes: Set[Config] = getRoutes): Map[String, Set[String]] = {
    routes.map(r => (r.getSource, r.getIgnoredField.toSet)).toMap
  }

  def getPrimaryKeys(routes: Set[Config] = getRoutes): Map[String, Set[String]] = {
    routes.map(r => (r.getSource, r.getPrimaryKeys.toSet)).toMap
  }

  def getTableTopic(routes: Set[Config] = getRoutes): Map[String, String] = {
    routes.map(r => (r.getSource, r.getTarget)).toMap
  }

  def getFormat(formatType: FormatType => FormatType, routes: Set[Config] = getRoutes): Map[String, FormatType] = {
    routes.map(r => (r.getSource, formatType(r.getFormatType))).toMap
  }
}
