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

import com.datamountaineer.kcql.Kcql
import scala.collection.JavaConverters._

case class Projections(targets: Map[String, String], // source -> target
                       headerFields: Map[String, Map[String, String]],
                       keyFields: Map[String, Map[String, String]],
                       valueFields: Map[String, Map[String, String]],
                       ignoreFields: Map[String, Set[String]]
                      )



object Projections {
  def apply(kcqls: Set[Kcql]): Projections = {
    Projections(
      targets = getTargetMapping(kcqls),
      headerFields = getHeaderFields(kcqls),
      keyFields = getKeyFields(kcqls),
      valueFields = getValueFields(kcqls),
      ignoreFields = kcqls.map(rm => (rm.getSource, rm.getIgnoredFields.asScala.map(f => f.getName).toSet)).toMap
    )
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