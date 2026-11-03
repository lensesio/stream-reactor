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
package io.lenses.streamreactor.connect.datalake.config

import cats.syntax.all._
import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password

import scala.util.Try

sealed trait AuthMode

object AuthMode {

  case class Credentials(
    accountName: String,
    accountKey:  Password,
  ) extends AuthMode

  case class ConnectionString(connectionString: String) extends AuthMode

  case class ServicePrincipal(
    clientId:     String,
    tenantId:     String,
    clientSecret: Password,
    accountName:  Option[String],
  ) extends AuthMode

  case object Default extends AuthMode

}

trait AuthModeSettingsConfigKeys extends WithConnectorPrefix {
  protected val AUTH_MODE: String = s"$connectorPrefix.azure.auth.mode"

  protected val AZURE_ACCOUNT_NAME:      String = s"$connectorPrefix.azure.account.name"
  protected val AZURE_ACCOUNT_KEY:       String = s"$connectorPrefix.azure.account.key"
  protected val AZURE_CONNECTION_STRING: String = s"$connectorPrefix.azure.connection.string"
  protected val AZURE_CLIENT_ID:         String = s"$connectorPrefix.azure.client.id"
  protected val AZURE_TENANT_ID:         String = s"$connectorPrefix.azure.tenant.id"
  protected val AZURE_CLIENT_SECRET:     String = s"$connectorPrefix.azure.client.secret"

  def withAuthModeSettings(configDef: ConfigDef): ConfigDef =
    configDef.define(
      AUTH_MODE,
      Type.STRING,
      AuthMode.Default.toString,
      Importance.HIGH,
      "Authenticate mode, 'credentials', 'connectionstring', 'serviceprincipal' or 'default'",
    )
      .define(
        AZURE_ACCOUNT_NAME,
        Type.STRING,
        "",
        Importance.HIGH,
        "Azure Account Name",
      )
      .define(
        AZURE_ACCOUNT_KEY,
        Type.PASSWORD,
        "",
        Importance.HIGH,
        "Azure Account Key",
      ).define(
        AZURE_CONNECTION_STRING,
        Type.PASSWORD,
        "",
        Importance.HIGH,
        "Azure Account Key",
      ).define(
        AZURE_CLIENT_ID,
        Type.STRING,
        "",
        Importance.HIGH,
        "Azure Entra ID application (client) id, used by the 'serviceprincipal' auth mode",
      ).define(
        AZURE_TENANT_ID,
        Type.STRING,
        "",
        Importance.HIGH,
        "Azure Entra ID directory (tenant) id, used by the 'serviceprincipal' auth mode",
      ).define(
        AZURE_CLIENT_SECRET,
        Type.PASSWORD,
        "",
        Importance.HIGH,
        "Azure Entra ID application client secret, used by the 'serviceprincipal' auth mode",
      )
}

trait AuthModeSettings extends BaseSettings with AuthModeSettingsConfigKeys {

  def getAuthMode: Either[Throwable, AuthMode] = {

    val authMode = Option(getString(AUTH_MODE)).map(_.trim.toLowerCase).filter(_.nonEmpty)
    authMode match {
      case Some("credentials") =>
        for {
          accName <- Try(getString(AZURE_ACCOUNT_NAME)).toEither
          pw      <- Try(getPassword(AZURE_ACCOUNT_KEY)).toEither
        } yield AuthMode.Credentials(accName, pw)
      case Some("connectionstring") =>
        for {
          cString <- Try(getPassword(AZURE_CONNECTION_STRING)).toEither
        } yield AuthMode.ConnectionString(cString.value())
      case Some("serviceprincipal") =>
        for {
          clientId     <- Try(getString(AZURE_CLIENT_ID)).toEither
          tenantId     <- Try(getString(AZURE_TENANT_ID)).toEither
          clientSecret <- Try(getPassword(AZURE_CLIENT_SECRET)).toEither
          accountName  <- Try(Option(getString(AZURE_ACCOUNT_NAME))).toEither
        } yield AuthMode.ServicePrincipal(
          clientId,
          tenantId,
          clientSecret,
          accountName.map(_.trim).filter(_.nonEmpty),
        )
      case Some("default")       => AuthMode.Default.asRight
      case Some(invalidAuthMode) => new ConfigException(s"Unsupported auth mode `$invalidAuthMode`").asLeft
      case None                  => AuthMode.Default.asRight
    }
  }

}
