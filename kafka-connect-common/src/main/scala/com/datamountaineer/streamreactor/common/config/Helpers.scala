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

package com.datamountaineer.streamreactor.common.config

import com.datamountaineer.kcql.Kcql
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigException

/**
  * Created by andrew@datamountaineer.com on 13/05/16.
  * kafka-connect-common
  */

object Helpers extends StrictLogging {

  /**
    * Build a mapping of table to topic
    * filtering on the assigned tables.
    *
    * @param input The raw input string to parse .i.e. table:topic,table2:topic2.
    * @param filterTable The tables to filter for.
    * @return a Map of table->topic.
    */
  def buildRouteMaps(input: String, filterTable: List[String]): Map[String, String] =
    tableTopicParser(input).filter({ case (k, _) => filterTable.contains(k) })

  //{table:f1,f2}
  def pKParser(input: String): Map[String, List[String]] = {
    val mappings = input.split("\\}")
      .toList
      .map(s => s.replace(",{", "").replace("{", "").replace("}", "").trim())

    mappings.map {
      m =>
        val colon = m.indexOf(":")
        if (colon >= 0) {
          val topic  = m.substring(0, colon)
          val fields = m.substring(colon + 1, m.length).split(",").toList
          (topic, fields)
        } else {
          throw new ConfigException(s"Invalid format for PKs. Received $input. Format should be {topic:f1,2}," +
            s"{topic2:f3,f3}....")
        }
    }
      .toMap
  }

  /**
    * Break a comma and colon separated string into a map of table to topic or topic to table
    *
    * If now values is found after a comma the value before the comma is used.
    *
    * @param input The input string to parse.
    * @return a Map of table->topic or topic->table.
    */
  def splitter(input: String, delimiter: String): Map[String, String] =
    input.split(",")
      .toList
      .map(c => c.split(delimiter))
      .map(a => if (a.length == 1) (a(0), a(0)) else (a(0), a(1))).toMap

  /**
    * Break a comma and colon separated string into a map of table to topic or topic to table
    *
    * If now values is found after a comma the value before the comma is used.
    *
    * @param input The input string to parse.
    * @return a Map of table->topic or topic->table.
    */
  def tableTopicParser(input: String): Map[String, String] =
    input.split(",")
      .toList
      .map(c => c.split(":"))
      .map(a => if (a.length == 1) (a(0), a(0)) else (a(0), a(1))).toMap

  def checkInputTopics(kcqlConstant: String, props: Map[String, String]): Boolean = {
    val topics = props("topics").split(",").map(t => t.trim).toSet
    val raw    = props(kcqlConstant)
    if (raw.isEmpty) {
      throw new ConfigException(s"Missing $kcqlConstant")
    }
    val kcql    = raw.split(";").map(r => Kcql.parse(r)).toSet
    val sources = kcql.map(k => k.getSource)
    val res     = topics.subsetOf(sources)

    if (!res) {
      val missing = topics.diff(sources)
      throw new ConfigException(
        s"Mandatory `topics` configuration contains topics not set in $kcqlConstant: ${missing}, kcql contains $sources",
      )
    }

    val res1 = sources.subsetOf(topics)

    if (!res1) {
      val missing = topics.diff(sources)
      throw new ConfigException(
        s"$kcqlConstant configuration contains topics not set in mandatory `topic` configuration: ${missing}, kcql contains $sources",
      )
    }

    true
  }
}
