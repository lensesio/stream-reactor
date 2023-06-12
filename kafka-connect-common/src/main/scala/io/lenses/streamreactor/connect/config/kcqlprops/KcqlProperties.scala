/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.config.kcqlprops

import cats.implicits.catsSyntaxOptionId
import enumeratum._

import scala.util.Try

object KcqlProperties {
  def fromStringMap[U <: EnumEntry, T <: Enum[U]](
    schema: KcqlPropsSchema[U, T],
    map:    Map[String, String],
  ): KcqlProperties[U, T] =
    new KcqlProperties(
      schema,
      map.map {
        case (k: String, v: String) => schema.keys.withNameInsensitive(k) -> v
      },
    )

}

case class KcqlProperties[U <: EnumEntry, T <: Enum[U]](
  schema: KcqlPropsSchema[U, T],
  map:    Map[U, String],
) {
  def getOptionalInt(key: U): Option[Int] =
    for {
      value:  String <- map.get(key)
      schema: PropsSchema <- schema.schema.get(key)
      _ <- schema match {
        case IntPropsSchema => value.some
        case _              => Option.empty[String]
      }
      i <- Try(value.toInt).toOption
    } yield i

  /*
  PKE - props enum (contains the prop keys)
  VE - target enum
   */
  def getEnumValue[VU <: EnumEntry, VT <: Enum[VU]](e: VT, key: U): Option[VU] =
    for {
      enumString: String <- map.get(key)
      enu <- e.withNameInsensitiveOption(enumString)
      //value <- schema.schema
    } yield enu

  def getString(key: U): Option[String] =
    for {
      value:  String <- map.get(key)
      schema: PropsSchema <- schema.schema.get(key)
      _ <- schema match {
        case StringPropsSchema => value.some
        case _                 => Option.empty[String]
      }
    } yield value

}
