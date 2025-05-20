/*
 * Copyright 2017-2025 Lenses.io Ltd
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
import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.connect.http.sink.client.oauth2.OAuth2Config
import org.apache.kafka.common.config.AbstractConfig
import org.http4s.Uri

import java.net.URL
import scala.jdk.CollectionConverters._
import scala.util.Try
sealed trait Authentication

object NoAuthentication extends Authentication
case class BasicAuthentication(username: String, password: String) extends Authentication
object BasicAuthentication {
  def from(configs: AbstractConfig, keys: AuthenticationKeys): Either[Throwable, Authentication] =
    for {
      username <- Option(configs.getString(keys.userNameKey)).toRight(
        new IllegalArgumentException(s"Basic authentication requires ${keys.userNameKey} to be set"),
      )
      password <- Option(configs.getPassword(keys.passwordKey)).map(_.value()).toRight(
        new IllegalArgumentException(s"Basic authentication requires ${keys.passwordKey} to be set"),
      )
    } yield BasicAuthentication(username, password)
}

case class OAuth2Authentication(
  tokenUrl:      Uri,
  clientId:      String,
  clientSecret:  String,
  tokenProperty: String,
  clientScope:   Option[String],
  clientHeaders: List[(String, String)],
) extends Authentication

object OAuth2Authentication {
  def from(configs: AbstractConfig): Either[Throwable, OAuth2Authentication] =
    for {
      tokenUrl <- Try(new URL(configs.getString(OAuth2Config.OAuth2TokenUrlProp)))
        .toEither
        .leftMap(_ =>
          new IllegalArgumentException(
            s"OAuth2 authentication requires ${OAuth2Config.OAuth2TokenUrlProp} to be set",
          ),
        )
      tokenUri <- Uri.fromString(tokenUrl.toString).leftMap(_ => new IllegalArgumentException("Invalid token URL"))
      clientId <- Option(configs.getString(OAuth2Config.OAuth2ClientIdProp)).toRight(
        new IllegalArgumentException(
          s"OAuth2 authentication requires ${OAuth2Config.OAuth2ClientIdProp} to be set",
        ),
      )
      clientSecret    = configs.getPassword(OAuth2Config.OAuth2ClientSecretProp).value()
      tokenProperty   = configs.getString(OAuth2Config.OAuth2AccessTokenFieldProp)
      scope           = Option(configs.getString(OAuth2Config.OAuth2ClientScopeProp))
      headerSeparator = configs.getString(OAuth2Config.OAuth2ClientHeadersSeparatorProp)
      headers = configs.getList(OAuth2Config.OAuth2ClientHeadersProp).asScala.map { header =>
        header.split(headerSeparator).toList match {
          case key :: value :: Nil => key -> value
          case _ =>
            throw new IllegalArgumentException(
              s"Invalid header format. Expected key${headerSeparator}value",
            )
        }
      }.toList
    } yield OAuth2Authentication(tokenUri, clientId, clientSecret, tokenProperty, scope, headers)
}

case class AuthenticationKeys(userNameKey: String, passwordKey: String, authTypeKey: String)

object Authentication {

  def from(configs: AbstractConfig, keys: AuthenticationKeys): Either[Throwable, Authentication] =
    Option(configs.getString(keys.authTypeKey)).map(_.trim.toLowerCase).map {
      case "basic"  => BasicAuthentication.from(configs, keys)
      case "none"   => Right(NoAuthentication)
      case "oauth2" => OAuth2Authentication.from(configs)
      case other =>
        Left(new IllegalArgumentException(s"Invalid authentication type: $other. Supported types are: basic, none"))
    }.getOrElse(NoAuthentication.asRight)
}
