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
package com.datamountaineer.streamreactor.connect.cassandra.cdc.logs

import java.lang

import scala.collection.JavaConversions._
import scala.collection.mutable

case class Offset(fileName: String,
                  location: Int,
                  descriptionId: Long) {

  def toMap(): mutable.HashMap[String, Object] = {
    val map = new mutable.HashMap[String, Object]()
    map.put("file", fileName)
    map.put("location", new Integer(location))
    map.put("id", new lang.Long(descriptionId))
    map
  }
}

object Offset {
  def from(map: java.util.Map[String, AnyRef]): Option[Offset] = {
    Option(map).map { m =>
      if (!m.containsKey("file")) {
        throw new IllegalArgumentException(s"Invalid state received. Missing state key:'file'. Available values are:${map.mkString(",")}")
      }

      if (!m.containsKey("location")) {
        throw new IllegalArgumentException(s"Invalid state received. Missing state key:'location'. Available values are:${map.mkString(",")}")
      }

      if (!m.containsKey("id")) {
        throw new IllegalArgumentException(s"Invalid state received. Missing state key:'location'. Available values are:${map.mkString(",")}")
      }
      val file = m.get("file").asInstanceOf[String]
      val location = m.get("location").asInstanceOf[Int]
      val id = m.get("id").asInstanceOf[Long]
      Offset(file, location, id)
    }
  }
}
