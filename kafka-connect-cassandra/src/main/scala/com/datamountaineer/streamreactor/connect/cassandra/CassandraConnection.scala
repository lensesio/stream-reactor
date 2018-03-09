/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.cassandra

import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigConstants
import com.datamountaineer.streamreactor.connect.config.{SSLConfig, SSLConfigContext}
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.{Cluster, JdkSSLOptions, QueryOptions, Session}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.AbstractConfig

import scala.io.Source

/**
  * Set up a Casssandra connection
  **/

object CassandraConnection extends StrictLogging {
  def apply(connectorConfig: AbstractConfig): CassandraConnection = {
    val cluster = getCluster(connectorConfig)
    val keySpace = connectorConfig.getString(CassandraConfigConstants.KEY_SPACE)
    val session = getSession(keySpace, cluster)
    new CassandraConnection(cluster = cluster, session = session)
  }

  def getCluster(connectorConfig: AbstractConfig): Cluster = {
    val contactPoints: String = connectorConfig.getString(CassandraConfigConstants.CONTACT_POINTS)
    val port = connectorConfig.getInt(CassandraConfigConstants.PORT)
    val fetchSize = connectorConfig.getInt(CassandraConfigConstants.FETCH_SIZE)
    val builder: Builder = Cluster
      .builder()
      .addContactPoints(contactPoints.split(","): _*)
      .withPort(port)
      .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
      .withQueryOptions(new QueryOptions().setFetchSize(fetchSize))

    //get authentication mode, only support NONE and USERNAME_PASSWORD for now
    addAuthMode(connectorConfig, builder)

    //is ssl enable
    addSSL(connectorConfig, builder)
    builder.build()
  }

  /**
    * Get a Cassandra session
    *
    * @param keySpace A configuration to build the setting from
    * @param cluster  The cluster the get the session for
    **/
  def getSession(keySpace: String, cluster: Cluster): Session = {
    cluster.connect(keySpace)
  }

  /**
    * Add authentication to the connection builder
    *
    * @param connectorConfig The connector configuration to get the parameters from
    * @param builder         The builder to add the authentication to
    * @return The builder with authentication added.
    **/
  private def addAuthMode(connectorConfig: AbstractConfig, builder: Builder): Builder = {
    val username = connectorConfig.getString(CassandraConfigConstants.USERNAME)

    val password = {
      if (connectorConfig.originals().containsKey(CassandraConfigConstants.PASSWD_FILE)) {
        val passwordFile = connectorConfig.getString(CassandraConfigConstants.PASSWD_FILE)
        Source.fromFile(passwordFile).getLines.mkString
      }
      else
        connectorConfig.getPassword(CassandraConfigConstants.PASSWD).value
    }

    if (username.length > 0 && password.length > 0) {
      builder.withCredentials(username.trim, password.toString.trim)
    } else {
      logger.warn("Username and password not set.")
    }
    builder
  }

  /**
    * Add SSL connection options to the connection builder
    *
    * @param connectorConfig The connector configuration to get the parameters from
    * @param builder         The builder to add the authentication to
    * @return The builder with SSL added.
    **/
  private def addSSL(connectorConfig: AbstractConfig, builder: Builder): Builder = {
    val ssl = connectorConfig.getBoolean(CassandraConfigConstants.SSL_ENABLED).asInstanceOf[Boolean]
    if (ssl) {
      logger.info("Setting up SSL context.")
      val sslConfig = SSLConfig(
        trustStorePath = connectorConfig.getString(CassandraConfigConstants.TRUST_STORE_PATH),
        trustStorePass = connectorConfig.getPassword(CassandraConfigConstants.TRUST_STORE_PASSWD).value,
        keyStorePath = Some(connectorConfig.getString(CassandraConfigConstants.KEY_STORE_PATH)),
        keyStorePass = Some(connectorConfig.getPassword(CassandraConfigConstants.KEY_STORE_PASSWD).value),
        useClientCert = connectorConfig.getBoolean(CassandraConfigConstants.USE_CLIENT_AUTH),
        keyStoreType = connectorConfig.getString(CassandraConfigConstants.KEY_STORE_TYPE),
        trustStoreType = connectorConfig.getString(CassandraConfigConstants.TRUST_STORE_TYPE)
      )

      val context = SSLConfigContext(sslConfig)
      //val cipherSuites: Array[String] = Array("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
      val sSLOptions = new JdkSSLOptions.Builder
      //sSLOptions.withCipherSuites(cipherSuites)
      sSLOptions.withSSLContext(context)
      builder.withSSL(sSLOptions.build())
    } else {
      builder
    }
  }
}

/**
  * <h1>CassandraConnection</h1>
  *
  * Case class to hold a Cassandra cluster and session connection
  **/
case class CassandraConnection(cluster: Cluster, session: Session)
