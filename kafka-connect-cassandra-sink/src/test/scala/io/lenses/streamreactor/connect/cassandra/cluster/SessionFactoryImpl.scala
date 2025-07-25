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
package io.lenses.streamreactor.connect.cassandra.cluster

import com.datastax.dse.driver.api.core.config.DseDriverOption
import com.datastax.dse.driver.api.core.config.DseDriverOption.GRAPH_SUB_PROTOCOL
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import com.datastax.oss.driver.api.core.config.DefaultDriverOption._
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.oss.driver.api.core.config.DriverOption
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider
import com.datastax.oss.dsbulk.tests.ccm.DefaultCCMCluster
import com.datastax.oss.dsbulk.tests.driver.factory.SessionFactory
import com.datastax.oss.dsbulk.tests.utils.SessionUtils
import com.datastax.oss.dsbulk.tests.utils.StringUtils
import com.google.common.collect.ImmutableMap
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import java.time.Duration
import java.util.Objects
import java.util.{ Map => JavaMap }
import scala.jdk.CollectionConverters._
class SessionFactoryImpl(config: SessionConfig, dcName: String) extends SessionFactory {
  import SessionFactoryImpl._

  private val useKeyspaceMode:    UseKeyspaceMode.Value = config.useKeyspace
  private val loggedKeyspaceName: String                = config.loggedKeyspaceName
  private val configLoader:       DriverConfigLoader    = buildConfigLoader(config, dcName)

  override def configureSession(session: CqlSession): Unit = {
    val keyspace = useKeyspaceMode match {
      case UseKeyspaceMode.NONE     => return
      case UseKeyspaceMode.GENERATE => StringUtils.uniqueIdentifier("ks")
      case UseKeyspaceMode.FIXED    => loggedKeyspaceName
    }
    SessionUtils.createSimpleKeyspace(session, keyspace)
    SessionUtils.useKeyspace(session, keyspace)
  }

  override def createSessionBuilder(): CqlSessionBuilder =
    CqlSession.builder().withConfigLoader(configLoader)

  override def equals(obj: Any): Boolean = obj match {
    case that: SessionFactoryImpl =>
      this.useKeyspaceMode == that.useKeyspaceMode &&
        Objects.equals(this.loggedKeyspaceName, that.loggedKeyspaceName) &&
        Objects.equals(this.configLoader.getInitialConfig, that.configLoader.getInitialConfig)
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hash(useKeyspaceMode, loggedKeyspaceName, configLoader.getInitialConfig)
}

private object SessionFactoryImpl {
  private val SSL_OPTIONS: JavaMap[DriverOption, String] = ImmutableMap.builder[DriverOption, String]()
    .put(SSL_ENGINE_FACTORY_CLASS, "DefaultSslEngineFactory")
    .put(SSL_TRUSTSTORE_PATH, DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_FILE.toString)
    .put(SSL_TRUSTSTORE_PASSWORD, DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
    .build()

  private val AUTH_OPTIONS: JavaMap[DriverOption, String] = ImmutableMap.builder[DriverOption, String]()
    .put(SSL_KEYSTORE_PATH, DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_FILE.toString)
    .put(SSL_KEYSTORE_PASSWORD, DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD)
    .build()

  private val ONE_MINUTE: Duration = Duration.ofSeconds(60)

  private def computeCredentials(credentials: Array[String]): Option[Array[String]] = {
    if (credentials.length != 0 && credentials.length != 2) {
      throw new IllegalArgumentException(
        "Credentials should be specified as an array of two elements (username and password)",
      )
    }
    if (credentials.length == 2) Some(credentials) else None
  }

  private def buildConfigLoader(config: SessionConfig, dcName: String): DriverConfigLoader = {
    val loaderBuilder = DriverConfigLoader.programmaticBuilder()
      .withInt(CONNECTION_POOL_LOCAL_SIZE, 1)
      .withInt(CONNECTION_POOL_REMOTE_SIZE, 1)
      .withString(LOAD_BALANCING_LOCAL_DATACENTER, dcName)
      .withBoolean(REQUEST_WARN_IF_SET_KEYSPACE, false)
      // set most timeouts to ridiculously high values
      .withDuration(REQUEST_TIMEOUT, ONE_MINUTE)
      .withDuration(CONNECTION_INIT_QUERY_TIMEOUT, ONE_MINUTE)
      .withDuration(CONNECTION_SET_KEYSPACE_TIMEOUT, ONE_MINUTE)
      .withDuration(METADATA_SCHEMA_REQUEST_TIMEOUT, ONE_MINUTE)
      .withDuration(HEARTBEAT_TIMEOUT, ONE_MINUTE)
      .withDuration(CONTROL_CONNECTION_TIMEOUT, ONE_MINUTE)
      .withDuration(CONTROL_CONNECTION_AGREEMENT_TIMEOUT, ONE_MINUTE)
      // speed up tests with aggressive Netty values
      .withInt(NETTY_IO_SHUTDOWN_QUIET_PERIOD, 0)
      .withInt(NETTY_IO_SHUTDOWN_TIMEOUT, 15)
      .withString(NETTY_IO_SHUTDOWN_UNIT, "SECONDS")
      .withInt(NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD, 0)
      .withInt(NETTY_ADMIN_SHUTDOWN_TIMEOUT, 15)
      .withString(NETTY_ADMIN_SHUTDOWN_UNIT, "SECONDS")
      .withString(GRAPH_SUB_PROTOCOL, "graph-binary-1.0")

    var settings: Config = ConfigFactory.empty()
    for (opt <- config.settings) {
      settings = ConfigFactory.parseString(opt).withFallback(settings)
    }

    settings.entrySet().asScala.foreach { entry =>
      setOption(loaderBuilder, optionFor(entry.getKey), entry.getValue.unwrapped())
    }

    computeCredentials(config.credentials).foreach { credentials =>
      loaderBuilder
        .withClass(AUTH_PROVIDER_CLASS, classOf[PlainTextAuthProvider])
        .withString(AUTH_PROVIDER_USER_NAME, credentials(0))
        .withString(AUTH_PROVIDER_PASSWORD, credentials(1))
    }

    if (config.auth) {
      AUTH_OPTIONS.asScala.foreach { case (key, value) =>
        setOption(loaderBuilder, key, value)
      }
    }

    if (config.ssl || config.auth) {
      SSL_OPTIONS.asScala.foreach { case (key, value) =>
        setOption(loaderBuilder, key, value)
      }
      setOption(loaderBuilder, SSL_HOSTNAME_VALIDATION, config.hostnameVerification)
    }

    loaderBuilder.build()
  }

  private def optionFor(key: String): DriverOption =
    DefaultDriverOption.values().find(_.getPath == key).getOrElse {
      DseDriverOption.values().find(_.getPath == key).getOrElse {
        throw new IllegalArgumentException(s"Unknown option: $key")
      }
    }

  private def setOption(loaderBuilder: ProgrammaticDriverConfigLoaderBuilder, option: DriverOption, value: Any): Unit =
    if (value == null) {
      loaderBuilder.without(option)
      ()
    } else {
      value match {
        case v: Boolean  => loaderBuilder.withBoolean(option, v)
        case v: Integer  => loaderBuilder.withInt(option, v)
        case v: Long     => loaderBuilder.withLong(option, v)
        case v: Double   => loaderBuilder.withDouble(option, v)
        case v: String   => loaderBuilder.withString(option, v)
        case v: Class[_] => loaderBuilder.withClass(option, v)
        case v: Duration => loaderBuilder.withDuration(option, v)
        case v: java.util.List[_] =>
          loaderBuilder.withBooleanList(option, v.asInstanceOf[java.util.List[java.lang.Boolean]])
        case v: java.util.Map[_, _] =>
          loaderBuilder.withStringMap(option, v.asInstanceOf[java.util.Map[String, String]])
        case _ => throw new IllegalArgumentException(s"Unknown option type: ${value.getClass}")
      }
      ()
    }
}
