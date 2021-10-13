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

import com.datamountaineer.kcql.{Bucketing, FormatType, Kcql, Tag, WriteModeEnum}
import com.datamountaineer.streamreactor.common.errors.{ErrorPolicy, ErrorPolicyEnum}
import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConverters._
import scala.collection.immutable.ListSet
import scala.util.{Failure, Success, Try}

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
                       sourceConverters: Map[String, Converter] = Map.empty,
                       formats: Map[String, FormatType] = Map.empty,
                       partitionBy: Map[String, Set[String]] = Map.empty,
                       tags: Map[String, Set[Tag]] = Map.empty,
                       withType: Map[String, String] = Map.empty,
                       dynamicTarget: Map[String, String] = Map.empty,
                       errorPolicy: ErrorPolicy = ErrorPolicy(ErrorPolicyEnum.THROW),
                       errorRetries: Int = 0,
                       kcqls: Set[Kcql] = Set.empty
                      )



object Projections extends StrictLogging {
  def apply(kcqls: Set[Kcql], props: Map[String, String], errorPolicy: ErrorPolicy, errorRetries: Int, defaultBatchSize: Int): Projections = {
    Projections(
      targets = getTargetMapping(kcqls),
      writeMode = getWriteMode(kcqls),
      headerFields = getHeaderFields(kcqls),
      keyFields = getKeyFields(kcqls),
      valueFields = getValueFields(kcqls),
      ignoreFields = kcqls.map(rm => (rm.getSource, rm.getIgnoredFields.asScala.map(f => f.getName).toSet)).toMap,
      primaryKeys = getPrimaryKeyCols(kcqls),
      formats = getFormat(kcqls),
      partitionBy = getPartitionByFields(kcqls),
      ttl = getTTL(kcqls),
      autoCreate = getAutoCreate(kcqls),
      autoEvolve = getAutoEvolve(kcqls),
      batchSize = getBatchSize(kcqls, defaultBatchSize),
      tags = getTags(kcqls),
      withType = getWithType(kcqls),
      sourceConverters = getSourceConverters(kcqls, props),
      errorPolicy = errorPolicy,
      errorRetries = errorRetries,
      kcqls = kcqls
    )
  }

  def getAutoCreate(kcql: Set[Kcql]): Map[String, Boolean] = {
    kcql.toList.map(r => (r.getSource, r.isAutoCreate)).toMap
  }

  def getAutoEvolve(kcql: Set[Kcql]): Map[String, Boolean] = {
    kcql.toList.map(r => (r.getSource, r.isAutoEvolve)).toMap
  }

  def getTTL(kcql: Set[Kcql]): Map[String, Long] = {
    kcql.toList.map(r => (r.getSource, r.getTTL)).toMap
  }

  def getPrimaryKeyCols(kcql: Set[Kcql]): Map[String, Set[String]] = {
    kcql.toList
      .map(k =>
        (k.getSource,
          ListSet(k.getPrimaryKeys.asScala.map(p => p.getName).reverse: _*).toSet))
      .toMap
  }

  def getBatchSize(kcql: Set[Kcql], defaultBatchSize: Int): Map[String, Int] = {
    kcql.toList
      .map(r =>
        (r.getSource, Option(r.getBatchSize).getOrElse(defaultBatchSize)))
      .toMap
  }


  def getFormat(kcql: Set[Kcql]): Map[String, FormatType] = {
    kcql.toList.map(r => (r.getSource, r.getFormatType)).toMap
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

  def getPartitionByFields(kcql: Set[Kcql]): Map[String, Set[String]] = {
    kcql.map { k => (k.getSource -> k.getPartitionBy.asScala.toSet) }.toMap
  }

  def getTags(kcql: Set[Kcql]): Map[String, Set[Tag]] = {
    kcql
      .filter(k => k.getTags != null)
      .map { k => (k.getSource -> k.getTags.asScala.toSet) }.toMap
  }

  def getWithType(kcql: Set[Kcql]): Map[String, String] = {
    kcql.map { k => (k.getSource -> k.getWithType) }.toMap
  }

  def getDynamicTarget(kcql: Set[Kcql]): Map[String, String] = {
    kcql.map { k => (k.getSource -> k.getDynamicTarget) }.toMap
  }

  def getDocType(kcql: Set[Kcql]): Map[String, String] = {
    kcql.map { k => (k.getSource -> k.getDocType) }.toMap
  }

  def getBucketing(kcql: Set[Kcql]): Map[String, Bucketing] = {
    kcql.map { k => (k.getSource -> k.getBucketing) }.toMap
  }

  def getKeyDelimiter(kcql: Set[Kcql]): Map[String, String] = {
    kcql.map { k => (k.getSource -> k.getKeyDelimeter) }.toMap
  }

  def getWithKeys(kcql: Set[Kcql]): Map[String, Set[String]] = {
    kcql.map { k => (k.getSource -> k.getWithKeys.asScala.toSet) }.toMap
  }

  def getSourceConverters(kcql: Set[Kcql], props: Map[String, String]): Map[String, Converter] = {
    kcql
      .filterNot(k => k.getWithConverter == null)
      .map(k => (k.getSource, k.getWithConverter))
      .map({
        case (source, clazz) =>
          logger.info(s"Creating converter instance for $clazz")
          val converter = Try(Class.forName(clazz).newInstance()) match {
            case Success(value) => value.asInstanceOf[Converter]
            case Failure(_) => throw new ConfigException(s"Invalid KCQL is invalid for [$source]. [$clazz] should have an empty ctor!")
          }
          converter.initialize(props)
          (source, converter)
    }).toMap
  }
}