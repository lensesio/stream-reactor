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

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import enumeratum._
import org.apache.kafka.common.config.ConfigException

import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object KcqlProperties {

  def stringToString: String => String = identity[String]

  def stringToInt: String => Int = _.toInt

  def normaliseCase[U <: EnumEntry, T <: Enum[U]](
    schema: KcqlPropsSchema[U, T],
    map:    Map[String, String],
  ): KcqlProperties[U, T] =
    new KcqlProperties(
      schema,
      map.map {
        case (k: String, v: String) => k.toLowerCase -> v
      },
    )
}

case class KcqlProperties[U <: EnumEntry, T <: Enum[U]](
  schema: KcqlPropsSchema[U, T],
  map:    Map[String, String],
) {
  def containsKeyStartingWith(str: String): Boolean = map.keys.exists(k => k.startsWith(str))

  def getOptionalInt(key: U): Option[Int] =
    for {
      value:  String <- map.get(key.entryName)
      schema: PropsSchema <- schema.schema.get(key)
      _ <- schema match {
        case IntPropsSchema => value.some
        case _              => Option.empty[String]
      }
      i <- Try(value.toInt).toOption
    } yield i

  def getOptionalChar(key: U): Option[Char] =
    for {
      value: Char <- map.get(key.entryName).filter(_.length == 1).flatMap(_.toCharArray.headOption)
      _:     PropsSchema <- schema.schema.get(key).filter(_ == CharPropsSchema)
    } yield value

  def getBooleanOrDefault(
    key:     U,
    default: Boolean,
  ): Either[ConfigException, Boolean] =
    map.get(key.entryName) match {
      case Some("true")  => true.asRight
      case Some("false") => false.asRight
      case Some(_) => new ConfigException(
          s"Invalid value for configuration [${key.entryName}]. The value must be one of: true, false.",
        ).asLeft[Boolean]
      case None => default.asRight
    }

  def getOptionalBoolean(key: U): Either[ConfigException, Option[Boolean]] = {
    val maybePropertyVal: Option[String] = map.get(key.entryName)
    val maybePropsSchema = schema.schema.get(key)
    (maybePropertyVal, maybePropsSchema) match {
      case (_, None) => new ConfigException(s"No schema found for ${key.entryName}").asLeft
      case (None, _) => Option.empty.asRight
      case (Some(propVal: String), Some(BooleanPropsSchema)) =>
        Try(propVal.toBoolean) match {
          case Failure(ex)    => new ConfigException(s"Invalid value for ${key.entryName}. Must be a boolean", ex).asLeft
          case Success(value) => value.some.asRight
        }
      case _ => new ConfigException(s"No schema found for ${key.entryName}").asLeft
    }
  }

  def getOptionalSet[V](key: U)(implicit converter: String => V, ct: ClassTag[V]): Option[Set[V]] =
    map.get(key.entryName) match {
      case Some(value) if schema.schema.get(key).contains(SetPropsSchema()) =>
        val elements = value.split(',').map(converter).toSet
        Some(elements)
      case _ => None
    }

  def getOptionalMap[K, V](keyPrefix: U, keyConverter: String => K, valueConverter: String => V): Option[Map[K, V]] = {
    val mapKeyPrefix = keyPrefix.entryName + "."
    val applicableEntries = map.collect {
      case (k, v)
          if k.startsWith(mapKeyPrefix) &&
            schema.schema.get(keyPrefix).contains(MapPropsSchema()) =>
        keyConverter(k.replace(mapKeyPrefix, "")) -> valueConverter(v)
    }
    Option.when(applicableEntries.nonEmpty)(applicableEntries)
  }

  /*
  PKE - props enum (contains the prop keys)
  VE - target enum
   */
  def getEnumValue[VU <: EnumEntry, VT <: Enum[VU]](e: VT, key: U): Option[VU] =
    for {
      enumString: String <- map.get(key.entryName)
      enu <- e.withNameInsensitiveOption(enumString)
      //value <- schema.schema
    } yield enu

  def getString(key: U): Option[String] =
    for {
      value:  String <- map.get(key.entryName)
      schema: PropsSchema <- schema.schema.get(key)
      _ <- schema match {
        case StringPropsSchema => value.some
        case _                 => Option.empty[String]
      }
    } yield value

}
