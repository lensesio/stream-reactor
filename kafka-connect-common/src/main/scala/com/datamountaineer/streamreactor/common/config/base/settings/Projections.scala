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

package com.datamountaineer.streamreactor.common.config.base.settings

import com.datamountaineer.kcql.{FormatType, Kcql, WriteModeEnum}
import com.datamountaineer.streamreactor.common.errors.{ErrorPolicy, ErrorPolicyEnum}
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConverters._
import scala.collection.immutable.ListSet

case class Projections(targets: Map[String, String], // source -> target
                       writeMode: Map[String, WriteModeEnum],
                       headerFields: Map[String, Map[String, String]],
                       keyFields: Map[String, Map[String, String]],
                       valueFields: Map[String, Map[String, String]],
                       ignoreFields: Map[String, Set[String]],
                       primaryKeys: Map[String, Set[String]],
                       storedAs: Map[String, String] = Map.empty,
                       format: Map[String, FormatType] = Map.empty,
                       ttl: Map[String, Long] = Map.empty,
                       batchSize: Map[String, Int] = Map.empty,
                       autoCreate: Map[String, Boolean] = Map.empty,
                       autoEvolve: Map[String, Boolean] = Map.empty,
                       converters: Map[String, String] = Map.empty,
                       errorPolicy: ErrorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
                       errorRetries: Int = 0,
                      )



object Projections {
  def apply(kcqls: Set[Kcql], errorPolicy: ErrorPolicy, errorRetries: Int): Projections = {
    Projections(
      targets = getTargetMapping(kcqls),
      writeMode = getWriteMode(kcqls),
      headerFields = getHeaderFields(kcqls),
      keyFields = getKeyFields(kcqls),
      valueFields = getValueFields(kcqls),
      ignoreFields = kcqls.map(rm => (rm.getSource, rm.getIgnoredFields.asScala.map(f => f.getName).toSet)).toMap,
      primaryKeys = getUpsertKeys(kcqls),
      errorPolicy = errorPolicy,
      errorRetries = errorRetries
    )
  }

  def getUpsertKeys(kcqls: Set[Kcql], preserveFullKeys: Boolean = false): Map[String, Set[String]] = {

    kcqls
      .filter(c => c.getWriteMode == WriteModeEnum.UPSERT)
      .map { r =>
        val keys: Set[String] = ListSet(
          r.getPrimaryKeys.asScala
            .map(key =>
              if (preserveFullKeys) {
                key.toString
              } else {
                key.getName
              })
            .reverse: _*)
        if (keys.isEmpty)
          throw new ConfigException(
            s"[${r.getTarget}] is set up with upsert, you need to set primary keys")
        (r.getSource, keys)
      }
      .toMap
  }

  def getWriteMode(kcql: Set[Kcql]): Map[String, WriteModeEnum] = {
    kcql.toList.map(r => (r.getSource, r.getWriteMode)).toMap
  }

  def getTargetMapping(kcql: Set[Kcql]) : Map[String, String] = {
    kcql.map { k => k.getSource -> k.getTarget}.toMap
  }

  def getValueFields(kcql: Set[Kcql]): Map[String, Map[String, String]] = {
    kcql.map { k =>
      k.getSource -> k.getFields.asScala
        .filterNot(f =>
          f.getName.startsWith("_key.") || f.getName.startsWith("_header."))
        .map(f => {
          val name = if (f.hasParents) {
            s"${f.getParentFields.asScala.mkString(".")}.${f.getName}"
          } else {
            f.getName
          }

          name -> f.getAlias
        })
        .toMap
    }.toMap
  }

  def getKeyFields(kcql: Set[Kcql]): Map[String, Map[String, String]] = {
    kcql.map { k =>
      k.getSource -> k.getFields.asScala
        .filter(f => f.getName.startsWith("_key."))
        .map(f => {
          val name = if (f.hasParents) {
            s"${f.getParentFields.asScala.mkString(".")}.${f.getName}"
          } else {
            f.getName
          }

          name.replaceFirst("_key.", "") -> f.getAlias
        }).toMap
    }.toMap
  }

  def getHeaderFields(kcql: Set[Kcql]): Map[String, Map[String, String]] = {
    kcql.map { k =>
      k.getSource -> k.getFields.asScala
        .filter(f => f.getName.startsWith("_header."))
        .map(f => {
          val name = if (f.hasParents) {
            s"${f.getParentFields.asScala.mkString(".")}.${f.getName}"
          } else {
            f.getName
          }

          name.replaceFirst("_header.", "") -> f.getAlias
        }).toMap
    }.toMap
  }
}