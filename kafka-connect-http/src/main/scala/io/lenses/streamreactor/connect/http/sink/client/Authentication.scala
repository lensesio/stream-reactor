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
package io.lenses.streamreactor.connect.http.sink.client

import cats.implicits.catsSyntaxEitherId

sealed trait Authentication

object NoAuthentication extends Authentication
case class BasicAuthentication(username: String, password: String) extends Authentication
object BasicAuthentication {
  def from(configs: Map[String, String], keys: AuthenticationKeys): Either[Throwable, Authentication] =
    for {
      username <- configs.get(keys.userNameKey).toRight(
        new IllegalArgumentException(s"Basic authentication requires ${keys.userNameKey} to be set"),
      )
      password <- configs.get(keys.passwordKey).toRight(
        new IllegalArgumentException(s"Basic authentication requires ${keys.passwordKey} to be set"),
      )
    } yield BasicAuthentication(username, password)
}

case class AuthenticationKeys(userNameKey: String, passwordKey: String, authTypeKey: String)

object Authentication {

  def from(configs: Map[String, String], keys: AuthenticationKeys): Either[Throwable, Authentication] =
    configs.get(keys.authTypeKey).map(_.trim.toLowerCase).map {
      case "basic" => BasicAuthentication.from(configs, keys)
      case "none"  => Right(NoAuthentication)
      case other =>
        Left(new IllegalArgumentException(s"Invalid authentication type: $other. Supported types are: basic, none"))
    }.getOrElse(NoAuthentication.asRight)
}
