/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.common.schemas

import org.apache.kafka.common.config.ConfigException

/**
  * Contains the SinkConnect payload fields to consider and/or their mappings
  *
  * @param includeAllFields Boolean fall to indicate if all fields are considered
  * @param fieldsMappings Field mappings from SinkRecord to HBase
  */
case class PayloadFields(includeAllFields: Boolean,
                         fieldsMappings: Map[String, String])

object PayloadFields {
  /**
    * Works out the fields and their mappings to be used when inserting a new row
    *
    * @param setting - The configuration specifing the fields and their mappings
    * @return A dictionary of fields and their mappings alongside a flag specifying if all fields should be used. If no mapping has been specified the field name is considered to be the mapping
    */
  def apply(setting: Option[String]): PayloadFields = {
    setting match {
      case None => PayloadFields(includeAllFields = true, Map.empty[String, String])
      case Some(c) =>

        val mappings = c.split(",").map { case f =>
          f.trim.split("=").toSeq match {
            case Seq(field) =>
              field -> field
            case Seq(field, alias) =>
              field -> alias
            case _ => throw new ConfigException(s"[$c] is not valid. Need to set the fields and mappings like: field1,field2,field3=alias3,[field4, field5=alias5]")
          }
        }.toMap

        PayloadFields(mappings.contains("*"), mappings - "*")
    }
  }
}
