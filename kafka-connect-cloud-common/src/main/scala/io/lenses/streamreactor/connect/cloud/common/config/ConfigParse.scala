/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import org.apache.kafka.common.config.types.Password

object ConfigParse {

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

  def getLong(props: Map[String, _], key: String): Option[Long] =
    props.get(key)
      .collect {
        case i: Int    => i.toLong
        case l: Long   => l
        case s: String => s.toLong
      }

  def getInt(props: Map[String, _], key: String): Option[Int] =
    props.get(key)
      .collect {
        case i: Int    => i
        case l: Long   => l.toInt
        case i: String => i.toInt
      }

}
