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

package com.datamountaineer.streamreactor.connect.blockchain.json

/*
object JacksonJson {
  val mapper = {
    (new ObjectMapper() with ScalaObjectMapper)
      .registerModule(DefaultScalaModule)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .setSerializationInclusion(Include.NON_NULL)
      .setSerializationInclusion(Include.NON_EMPTY)

  }

  implicit class JsonStringToTypeConverter(val json: String) extends AnyVal {
    def to[T: ClassTag](): T = mapper.readValue(json, implicitly[ClassTag[T]].runtimeClass).asInstanceOf[T]
  }

}
*/

import org.json4s._
import org.json4s.native.JsonMethods._

object JacksonJson {
  implicit val formats = DefaultFormats

  def fromJson[T <: Product:Manifest](json: String): T = {
    parse(json).extract[T]
  }
}
