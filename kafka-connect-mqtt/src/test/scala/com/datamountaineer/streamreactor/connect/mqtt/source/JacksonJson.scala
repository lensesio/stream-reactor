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

package com.datamountaineer.streamreactor.connect.mqtt.source

import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

object JacksonJson {

  //implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  /*def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }*/

  def toJson[T <: AnyRef](value: T): String = {
    write(value)
  }
}
