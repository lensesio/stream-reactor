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
package io.lenses.streamreactor.connect.cloud.common.config

import cats.implicits.catsSyntaxEitherId
import cats.implicits.toBifunctorOps
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.connect.errors.ConnectException

import scala.util.Try

trait PropertiesHelper {

  def getStringEither(props: Map[String, _], key: String): Either[Throwable, String] =
    getString(props, key)
      .toRight(new ConnectException(s"Configuration is missing the setting for [$key]."))

  def getString(props: Map[String, _], key: String): Option[String] =
    props.get(key)
      .collect {
        case s: String if s.nonEmpty => s
      }

  def getPassword(props: Map[String, _], key: String): Option[String] =
    props.get(key)
      .collect {
        case p: Password if p.value().nonEmpty => p.value()
        case s: String if s.nonEmpty           => s
      }

  def getBoolean(props: Map[String, _], key: String): Option[Boolean] =
    props.get(key)
      .collect {
        case b: Boolean => b
        case "true"  => true
        case "false" => false
      }

  def getLong(props: Map[String, _], key: String): Option[Long] = getLongEither(props, key).toOption

  def getLongEither(props: Map[String, _], key: String): Either[Throwable, Long] =
    props.get(key) match {
      case Some(value) =>
        value match {
          case i: Int => i.toLong.asRight[Throwable]
          case l: Long => l.asRight[Throwable]
          case s: String =>
            Try(s.toLong).toEither.leftMap(_ =>
              new ConnectException(s"Configuration for setting [$key] is not a valid long."),
            )
          case _ => new ConnectException(s"Configuration for setting [$key] is not a valid long.").asLeft[Long]
        }
      case None => new ConnectException(s"Configuration is missing the setting for [$key].").asLeft[Long]
    }
  def getInt(props: Map[String, _], key: String): Option[Int] = getIntEither(props, key).toOption

  def getIntEither(props: Map[String, _], key: String): Either[Throwable, Int] =
    props.get(key) match {
      case Some(value) =>
        value match {
          case i: Int => i.asRight[Throwable]
          case l: Long => l.toInt.asRight[Throwable]
          case i: String =>
            Try(i.toInt).toEither.leftMap(_ =>
              new ConnectException(s"Configuration for setting [$key] is not a valid integer."),
            )
          case _ => new ConnectException(s"Configuration for setting [$key] is not a valid integer.").asLeft[Int]
        }
      case None => new ConnectException(s"Configuration is missing the setting for [$key].").asLeft[Int]
    }

}
