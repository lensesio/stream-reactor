/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.common.config.base.traits

import com.datamountaineer.kcql.Field
import com.datamountaineer.kcql.FormatType
import com.datamountaineer.kcql.Kcql
import com.datamountaineer.kcql.WriteModeEnum
import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst.KCQL_PROP_SUFFIX
import com.datamountaineer.streamreactor.common.rowkeys.StringGenericRowKeyBuilder
import com.datamountaineer.streamreactor.common.rowkeys.StringKeyBuilder
import com.datamountaineer.streamreactor.common.rowkeys.StringStructFieldsStringKeyBuilder
import org.apache.kafka.common.config.ConfigException

import scala.collection.immutable.ListSet
import scala.jdk.CollectionConverters.ListHasAsScala

trait KcqlSettings extends BaseSettings {
  val kcqlConstant: String = s"$connectorPrefix.$KCQL_PROP_SUFFIX"

  def getKCQL: Set[Kcql] = {
    val raw = getString(kcqlConstant)
    if (raw.isEmpty) {
      throw new ConfigException(s"Missing [$kcqlConstant]")
    }
    raw.split(";").map(r => Kcql.parse(r)).toSet
  }

  def getKCQLRaw: Array[String] = {
    val raw = getString(kcqlConstant)
    if (raw.isEmpty) {
      throw new ConfigException(s"Missing [$kcqlConstant]")
    }
    raw.split(";")
  }

  def getFieldsMap(
    kcql: Set[Kcql] = getKCQL,
  ): Map[String, Map[String, String]] =
    kcql.toList
      .map(rm => (rm.getSource, rm.getFields.asScala.map(fa => (fa.toString, fa.getAlias)).toMap))
      .toMap

  def getFields(kcql: Set[Kcql] = getKCQL): Map[String, Seq[Field]] =
    kcql.toList.map(rm => (rm.getSource, rm.getFields.asScala.toSeq)).toMap

  def getIgnoreFields(kcql: Set[Kcql] = getKCQL): Map[String, Seq[Field]] =
    kcql.toList.map(rm => (rm.getSource, rm.getIgnoredFields.asScala.toSeq)).toMap

  def getFieldsAliases(kcql: Set[Kcql] = getKCQL): List[Map[String, String]] =
    kcql.toList.map(rm => rm.getFields.asScala.map(fa => (fa.getName, fa.getAlias)).toMap)

  def getIgnoreFieldsMap(
    kcql: Set[Kcql] = getKCQL,
  ): Map[String, Set[String]] =
    kcql.toList
      .map(r => (r.getSource, r.getIgnoredFields.asScala.map(f => f.getName).toSet))
      .toMap

  def getPrimaryKeys(kcql: Set[Kcql] = getKCQL): Map[String, Set[String]] =
    kcql.toList.map { r =>
      val names: Seq[String] = r.getPrimaryKeys.asScala.map(f => f.getName).toSeq
      val set:   Set[String] = ListSet(names.reverse: _*)
      (r.getSource, set)
    }.toMap

  def getTableTopic(kcql: Set[Kcql] = getKCQL): Map[String, String] =
    kcql.toList.map(r => (r.getSource, r.getTarget)).toMap

  def getFormat(formatType: FormatType => FormatType, kcql: Set[Kcql] = getKCQL): Map[String, FormatType] =
    kcql.toList.map(r => (r.getSource, formatType(r.getFormatType))).toMap

  def getTTL(kcql: Set[Kcql] = getKCQL): Map[String, Long] =
    kcql.toList.map(r => (r.getSource, r.getTTL)).toMap

//  def getIncrementalMode(kcql: Set[Kcql] = getKCQL): Map[String, String] = {
//    kcql.toList.map(r => (r.getSource, r.getIncrementalMode)).toMap
//  }

  def getBatchSize(kcql: Set[Kcql] = getKCQL, defaultBatchSize: Int): Map[String, Int] =
    kcql.toList
      .map(r => (r.getSource, Option(r.getBatchSize).getOrElse(defaultBatchSize)))
      .toMap

  def getBucketSize(kcql: Set[Kcql] = getKCQL): Map[String, Int] =
    kcql.toList.map(r => (r.getSource, r.getBucketing.getBucketsNumber)).toMap

  def getWriteMode(kcql: Set[Kcql] = getKCQL): Map[String, WriteModeEnum] =
    kcql.toList.map(r => (r.getSource, r.getWriteMode)).toMap

  def getAutoCreate(kcql: Set[Kcql] = getKCQL): Map[String, Boolean] =
    kcql.toList.map(r => (r.getSource, r.isAutoCreate)).toMap

  def getAutoEvolve(kcql: Set[Kcql] = getKCQL): Map[String, Boolean] =
    kcql.toList.map(r => (r.getSource, r.isAutoEvolve)).toMap

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

  def getUpsertKey(kcql: Set[Kcql] = getKCQL): Map[String, String] =
    kcql
      .filter(c => c.getWriteMode == WriteModeEnum.UPSERT)
      .map { r =>
        val keyList: List[Field] = r.getPrimaryKeys.asScala.toList
        val keys:    Set[Field]  = ListSet(keyList.reverse: _*)
        if (keys.isEmpty)
          throw new ConfigException(
            s"[${r.getTarget}] is set up with upsert, you need to set primary keys",
          )
        (r.getSource, keys.head.getName)
      }
      .toMap

  def getRowKeyBuilders(kcql: Set[Kcql] = getKCQL): List[StringKeyBuilder] =
    kcql.toList.map { k =>
      val keys = k.getPrimaryKeys.asScala.map(k => k.getName).toSeq
      // No PK => 'topic|par|offset' builder else generic-builder
      if (keys.nonEmpty) StringStructFieldsStringKeyBuilder(keys)
      else new StringGenericRowKeyBuilder()
    }

  def getPrimaryKeyCols(kcql: Set[Kcql] = getKCQL): Map[String, Set[String]] =
    kcql.toList
      .map(k => (k.getSource, ListSet(k.getPrimaryKeys.asScala.map(p => p.getName).reverse.toSeq: _*).toSet))
      .toMap

  def getIncrementalMode(routes: Set[Kcql]): Map[String, String] =
    routes.toList.map(r => (r.getSource, r.getIncrementalMode)).toMap

  def checkInputTopics(props: Map[String, String]): Boolean = {
    val topics = props("topics").split(",").toSet
    val raw    = props(kcqlConstant)
    if (raw.isEmpty) {
      throw new ConfigException(s"Missing [$kcqlConstant]")
    }
    val kcql    = raw.split(";").map(r => Kcql.parse(r)).toSet
    val sources = kcql.map(k => k.getSource)

    val res = topics.subsetOf(sources)

    if (!res) {
      throw new ConfigException(
        s"Mandatory `topics` configuration contains topics not set in [$kcqlConstant]",
      )
    }

    val res1 = sources.subsetOf(topics)

    if (!res1) {
      throw new ConfigException(
        s"[$kcqlConstant] configuration contains topics not set in mandatory `topic` configuration",
      )
    }

    true
  }
}
