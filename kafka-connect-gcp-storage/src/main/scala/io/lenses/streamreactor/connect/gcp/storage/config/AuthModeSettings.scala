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
package io.lenses.streamreactor.connect.gcp.storage.config

import cats.syntax.all._
import com.datamountaineer.streamreactor.common.config.base.traits.BaseSettings
import com.datamountaineer.streamreactor.common.config.base.traits.WithConnectorPrefix
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password

import scala.util.Try

sealed trait AuthMode

object AuthMode {

  /**
    * Authentication mode using credentials from a string in configuration.
    *
    * @param credentials The credentials used for authentication.
    */
  case class Credentials(credentials: Password) extends AuthMode

  /**
    * Authentication mode using a json file for credentials.
    *
    * @param filePath The path to the file containing json credentials.
    */
  case class File(filePath: String) extends AuthMode

  /**
    * Default authentication mode without explicit credentials. This mode utilizes the Application Default Credentials (ADC) chain.
    * ADC is a strategy used by the Google authentication libraries to automatically find credentials based on the application environment.
    * The credentials are made available to Cloud Client Libraries and Google API Client Libraries, allowing the code to run seamlessly
    * in both development and production environments without altering the authentication process for Google Cloud services and APIs.
    */
  case object Default extends AuthMode

  /**
    * Authentication mode indicating no authentication is required.
    */
  case object None extends AuthMode

}

trait AuthModeSettingsConfigKeys extends WithConnectorPrefix {
  protected val AUTH_MODE:   String = s"$connectorPrefix.gcp.auth.mode"
  protected val CREDENTIALS: String = s"$connectorPrefix.gcp.credentials"
  protected val FILE:        String = s"$connectorPrefix.gcp.file"

  def withAuthModeSettings(configDef: ConfigDef): ConfigDef =
    configDef.define(
      AUTH_MODE,
      Type.STRING,
      AuthMode.Default.toString,
      Importance.HIGH,
      "Authenticate mode, 'credentials', 'file' or 'default'",
    )
      .define(
        CREDENTIALS,
        Type.STRING,
        "",
        Importance.HIGH,
        "GCP Credentials if using 'credentials' auth mode.",
      )
      .define(
        FILE,
        Type.STRING,
        "",
        Importance.HIGH,
        "File containing GCP Credentials if using 'file' auth mode",
      )
}

trait AuthModeSettings extends BaseSettings with AuthModeSettingsConfigKeys {

  def getAuthMode: Either[Throwable, AuthMode] = {

    val authMode = Option(getString(AUTH_MODE)).map(_.trim.toLowerCase).filter(_.nonEmpty)
    authMode match {
      case Some("credentials") =>
        for {
          creds <- Try(getPassword(CREDENTIALS)).toEither
        } yield AuthMode.Credentials(creds)
      case Some("file") =>
        for {
          filePath <- Try(getString(FILE)).toEither
        } yield AuthMode.File(filePath)
      case Some("default")       => AuthMode.Default.asRight
      case Some("none")          => AuthMode.None.asRight
      case Some(invalidAuthMode) => new ConfigException(s"Unsupported auth mode `$invalidAuthMode`").asLeft
      case None                  => AuthMode.Default.asRight
    }
  }

}
