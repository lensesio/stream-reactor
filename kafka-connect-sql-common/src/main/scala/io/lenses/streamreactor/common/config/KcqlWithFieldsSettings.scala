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
package io.lenses.streamreactor.common.config

import io.lenses.kcql.Kcql
import io.lenses.kcql.WriteModeEnum
import io.lenses.streamreactor.common.config.base.traits.KcqlSettings
import org.apache.kafka.common.config.ConfigException

import scala.collection.immutable.ListSet
import scala.jdk.CollectionConverters.ListHasAsScala

trait KcqlWithFieldsSettings extends KcqlSettings {

  def getFieldsMap(
    kcql: Set[Kcql] = getKCQL,
  ): Map[String, Map[String, String]] =
    kcql.toList
      .map(rm => (rm.getSource, rm.getFields.asScala.map(fa => (fa.toString, fa.getAlias)).toMap))
      .toMap

  def getIgnoreFieldsMap(
    kcql: Set[Kcql] = getKCQL,
  ): Map[String, Set[String]] =
    kcql.toList
      .map(r => (r.getSource, r.getIgnoredFields.asScala.map(f => f.getName).toSet))
      .toMap

  /** Get all the upsert keys
    *
    * @param kcql
    * @param preserveFullKeys (default false) If true, keys that
    *                         have parents will return the full
    *                         key (ie. "A.B.C" rather than just
    *                         "C")
    * @return map of topic to set of keys
    */
  def getUpsertKeys(
    kcql:             Set[Kcql] = getKCQL,
    preserveFullKeys: Boolean   = false,
  ): Map[String, Set[String]] =
    kcql
      .filter(c => c.getWriteMode == WriteModeEnum.UPSERT)
      .map { r =>
        val keys: Set[String] = ListSet(
          r.getPrimaryKeys.asScala.toSeq
            .map(key =>
              preserveFullKeys match {
                case false => key.getName
                case true  => key.toString
              },
            )
            .reverse: _*,
        )
        if (keys.isEmpty)
          throw new ConfigException(
            s"[${r.getTarget}] is set up with upsert, you need to set primary keys",
          )
        (r.getSource, keys)
      }
      .toMap

}
