/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.http.sink.tpl.substitutions

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Schema.Type
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

import java.util.Base64
import scala.util.Try
import scala.jdk.CollectionConverters._

object HeaderValueConverter {
  implicit val formats: org.json4s.Formats = Serialization.formats(NoTypeHints)

  def headerValueToString(value: Any, schema: Schema): String = {
    val valueOpt      = Option(value)
    val schemaOpt     = Option(schema)
    val schemaTypeOpt = schemaOpt.map(_.`type`)
    (valueOpt, schemaOpt, schemaTypeOpt) match {
      case (None, _, _)                          => ""
      case (Some(v), Some(_), Some(Type.STRING)) => v.toString
      case (Some(v), Some(_), Some(Type.BYTES)) => v match {
          case arr: Array[Byte] => Base64.getEncoder.encodeToString(arr)
          case _ => v.toString
        }
      case (Some(v), Some(_), Some(tpe)) if Set(Type.STRUCT, Type.ARRAY).contains(tpe) =>
        Try(write(v)).getOrElse(v.toString)
      case (Some(v), Some(_), Some(Type.MAP)) =>
        // Convert Java Map to Scala Map for json4s
        val asScala = v match {
          case jm: java.util.Map[_, _] => jm.asInstanceOf[java.util.Map[Any, Any]].asScala.toMap
          case _ => v
        }
        Try(write(asScala)).getOrElse(v.toString)
      case (Some(v), Some(_), Some(_)) => v.toString
      case (Some(v), Some(_), None)    => v.toString
      case (Some(v), None, _)          => v.toString
    }
  }
}
