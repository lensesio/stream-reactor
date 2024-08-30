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
package io.lenses.streamreactor.common.config

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException

import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Paths
import java.security.KeyStore
import java.security.SecureRandom
import javax.net.ssl._
import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 14/04/16.
  * stream-reactor
  */
object SSLConfigContext {
  def apply(config: SSLConfig): SSLContext =
    getSSLContext(config)

  /**
    * Get a SSL Connect for a given set of credentials
    *
    * @param config An SSLConfig containing key and truststore credentials
    * @return a SSLContext
    */
  private def getSSLContext(config: SSLConfig): SSLContext = {
    val useClientCertAuth = config.keyStorePath.isDefined && config.keyStorePass.isDefined

    //is client certification authentication set
    val keyManagers: Array[KeyManager] = if (useClientCertAuth) {
      getKeyManagers(config)
    } else {
      Array[KeyManager]()
    }

    val ctx: SSLContext = SSLContext.getInstance("SSL")
    val trustManagers = getTrustManagers(config)
    ctx.init(keyManagers, trustManagers, new SecureRandom())
    ctx
  }

  /**
    * Get an array of Trust Managers
    *
    * @param config An SSLConfig containing key and truststore credentials
    * @return An Array of TrustManagers
    */
  def getTrustManagers(config: SSLConfig): Array[TrustManager] = {
    val tsf = new FileInputStream(config.trustStorePath)
    val ts  = KeyStore.getInstance(config.trustStoreType)
    ts.load(tsf, config.trustStorePass.toCharArray)
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(ts)
    tmf.getTrustManagers
  }

  /**
    * Get an array of Key Managers
    *
    * @param config An SSLConfig containing key and truststore credentials
    * @return An Array of KeyManagers
    */
  def getKeyManagers(config: SSLConfig): Array[KeyManager] = {
    require(config.keyStorePath.nonEmpty, "Key store path is not set!")
    require(config.keyStorePass.nonEmpty, "Key store password is not set!")
    val ksf = new FileInputStream(config.keyStorePath.get)
    val ks  = KeyStore.getInstance(config.keyStoreType)
    ks.load(ksf, config.keyStorePass.get.toCharArray)
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(ks, config.keyStorePass.get.toCharArray)
    kmf.getKeyManagers
  }

}

/**
  * Class for holding key and truststore settings
  */
case class SSLConfig(
  trustStorePath: String,
  trustStorePass: String,
  keyStorePath:   Option[String],
  keyStorePass:   Option[String],
  keyStoreType:   String = "JKS",
  trustStoreType: String = "JKS",
)

private case class SSLConfigProps(prefix: String) {
  val SSLEnabledProp: String = s"$prefix.ssl.enabled"
  val SSLEnabledDoc: String =
    """
      |Whether to enable SSL.
      |""".stripMargin

  val SSLTrustStorePathProp: String = s"$prefix.ssl.truststore.path"
  val SSLTrustStorePathDoc: String =
    """
      |The path to the truststore.
      |""".stripMargin

  val SSLTrustStorePasswordProp: String = s"$prefix.ssl.truststore.password"
  val SSLTrustStorePassDoc: String =
    """
      |The password for the truststore.
      |""".stripMargin

  val SSLKeyStorePathProp: String = s"$prefix.ssl.keystore.path"
  val SSLKeyStorePathDoc: String =
    """
      |The path to the keystore.
      |""".stripMargin

  val SSLKeyStorePasswordProp: String = s"$prefix.ssl.keystore.password"
  val SSLKeyStorePassDoc: String =
    """
      |The password for the keystore.
      |""".stripMargin

  val SSLKeyStoreTypeProp: String = s"$prefix.ssl.keystore.type"
  val SSLKeyStoreTypeDoc: String =
    """
      |The type of the keystore.
      |""".stripMargin

  val SSLTrustStoreTypeProp: String = s"$prefix.ssl.truststore.type"
  val SSLTrustStoreTypeDoc: String =
    """
      |The type of the truststore.
      |""".stripMargin
}
object SSLConfig {

  def appendConfig(config: ConfigDef, prefix: String): ConfigDef =
    config.define(
      SSLConfigProps(prefix).SSLEnabledProp,
      ConfigDef.Type.BOOLEAN,
      false,
      ConfigDef.Importance.HIGH,
      SSLConfigProps(prefix).SSLEnabledDoc,
    )
      .define(
        SSLConfigProps(prefix).SSLTrustStorePathProp,
        ConfigDef.Type.STRING,
        null,
        ConfigDef.Importance.HIGH,
        SSLConfigProps(prefix).SSLTrustStorePathDoc,
      )
      .define(
        SSLConfigProps(prefix).SSLTrustStorePasswordProp,
        ConfigDef.Type.PASSWORD,
        null,
        ConfigDef.Importance.HIGH,
        SSLConfigProps(prefix).SSLTrustStorePassDoc,
      )
      .define(
        SSLConfigProps(prefix).SSLKeyStorePathProp,
        ConfigDef.Type.STRING,
        null,
        ConfigDef.Importance.LOW,
        SSLConfigProps(prefix).SSLKeyStorePathDoc,
      )
      .define(
        SSLConfigProps(prefix).SSLKeyStorePasswordProp,
        ConfigDef.Type.PASSWORD,
        null,
        ConfigDef.Importance.LOW,
        SSLConfigProps(prefix).SSLKeyStorePassDoc,
      )
      .define(
        SSLConfigProps(prefix).SSLKeyStoreTypeProp,
        ConfigDef.Type.STRING,
        "JKS",
        ConfigDef.Importance.HIGH,
        SSLConfigProps(prefix).SSLKeyStoreTypeDoc,
      )
      .define(
        SSLConfigProps(prefix).SSLTrustStoreTypeProp,
        ConfigDef.Type.STRING,
        "JKS",
        ConfigDef.Importance.HIGH,
        SSLConfigProps(prefix).SSLTrustStoreTypeDoc,
      )

  def from(config: AbstractConfig, prefix: String): Either[Throwable, Option[SSLConfig]] = {
    val sslConfigProps = SSLConfigProps(prefix)

    def validateFilePath(path: String, propName: String): Either[Throwable, String] =
      Option(path).map(_.trim).filter(_.nonEmpty)
        .toRight(new IllegalArgumentException(s"File path is not set for $propName")).flatMap { path =>
          if (Files.exists(Paths.get(path))) Right(path)
          else Left(new IllegalArgumentException(s"File $path does not exist for property:$propName."))
        }

    def getConfigValue[T](getter: => T, propName: String): Either[Throwable, T] =
      Try(getter).toEither.left.map(e => new ConfigException(s"Missing $propName property", e))

    def validateOptionalPath(pathOpt: Option[String], propName: String): Either[Throwable, Option[String]] =
      pathOpt match {
        case Some(path) => validateFilePath(path, propName).map(Some(_))
        case None       => Right(None)
      }

    for {
      sslEnabled <- getConfigValue(config.getBoolean(sslConfigProps.SSLEnabledProp), sslConfigProps.SSLEnabledProp)
      sslConfig <- if (sslEnabled) {
        for {
          trustStorePath <- getConfigValue(config.getString(sslConfigProps.SSLTrustStorePathProp),
                                           sslConfigProps.SSLTrustStorePathProp,
          )
          validTrustStorePath <- validateFilePath(trustStorePath, sslConfigProps.SSLTrustStorePathProp)
          trustStorePass <- getConfigValue(config.getPassword(sslConfigProps.SSLTrustStorePasswordProp).value(),
                                           sslConfigProps.SSLTrustStorePasswordProp,
          )
          keyStorePath <- getConfigValue(Option(config.getString(sslConfigProps.SSLKeyStorePathProp)),
                                         sslConfigProps.SSLKeyStorePathProp,
          )
          validKeyStorePath <- validateOptionalPath(keyStorePath, sslConfigProps.SSLKeyStorePathProp)
          keyStorePass <- getConfigValue(
            Option(config.getPassword(sslConfigProps.SSLKeyStorePasswordProp)).map(_.value()),
            sslConfigProps.SSLKeyStorePasswordProp,
          )

          keyStoreType <- getConfigValue(config.getString(sslConfigProps.SSLKeyStoreTypeProp),
                                         sslConfigProps.SSLKeyStoreTypeProp,
          )
          trustStoreType <- getConfigValue(config.getString(sslConfigProps.SSLTrustStoreTypeProp),
                                           sslConfigProps.SSLTrustStoreTypeProp,
          )
        } yield Some(SSLConfig(validTrustStorePath,
                               trustStorePass,
                               validKeyStorePath,
                               keyStorePass,
                               keyStoreType,
                               trustStoreType,
        ))
      } else {
        Right(None)
      }
    } yield sslConfig
  }
}
