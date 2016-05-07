/**
  * Copyright 2015 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.cassandra

import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigConstants
import com.datamountaineer.streamreactor.connect.config.{SSLConfig, SSLConfigContext}
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, JdkSSLOptions, QueryOptions, Session}
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.AbstractConfig

/**
  * Set up a Casssandra connection
  * */

object CassandraConnection extends StrictLogging {
  def apply(connectorConfig: AbstractConfig) : CassandraConnection = {
      val session = getSession(connectorConfig)
      new CassandraConnection(session)
    }

  /**
    * Get a Cassandra session
    *
    * @param connectorConfig A configuration to build the setting from
    * */
  def getSession(connectorConfig: AbstractConfig) : Session = {
    val contactPoints: String = connectorConfig.getString(CassandraConfigConstants.CONTACT_POINTS)
    val port = connectorConfig.getInt(CassandraConfigConstants.PORT)
    val keySpace = connectorConfig.getString(CassandraConfigConstants.KEY_SPACE)

    logger.info(s"Attempting to connect to Cassandra cluster at $contactPoints and create keyspace $keySpace.")

    val builder: Builder = Cluster
      .builder()
      .addContactPoints(contactPoints)
      .withPort(port)
      .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))

    //set query options
    val queryOptions = new QueryOptions
    queryOptions.setFetchSize(connectorConfig.getInt(CassandraConfigConstants.FETCH_SIZE))
    builder.withQueryOptions(queryOptions)

    //get authentication mode, only support NONE and USERNAME_PASSWORD for now
    addAuthMode(connectorConfig, builder)

    //is ssl enable
    addSSL(connectorConfig, builder)

    val cluster = builder.build()
    cluster.connect(keySpace)
  }

  /**
    * Add authentication to the connection builder
    *
    * @param connectorConfig The connector configuration to get the parameters from
    * @param builder The builder to add the authentication to
    * @return The builder with authentication added.
    * */
  private def addAuthMode(connectorConfig: AbstractConfig, builder: Builder) : Builder = {
    val authMode = connectorConfig.getString(CassandraConfigConstants.AUTHENTICATION_MODE).toLowerCase()
    authMode match {
      case CassandraConfigConstants.USERNAME_PASSWORD =>
        logger.info(s"Using ${CassandraConfigConstants.USERNAME_PASSWORD}.")
        val username = connectorConfig.getString(CassandraConfigConstants.USERNAME)
        val password = connectorConfig.getPassword(CassandraConfigConstants.PASSWD).value
        require(username.nonEmpty, s"Authentication mode set to $CassandraConfigConstants.USERNAME_PASSWORD but no username supplied.")

        builder.withCredentials(username.trim, password.toString.trim)
      case CassandraConfigConstants.AUTHENTICATION_MODE_DEFAULT =>
    }
    builder
  }

  /**
    * Add SSL connection options to the connection builder
    * @param connectorConfig The connector configuration to get the parameters from
    * @param builder The builder to add the authentication to
    * @return The builder with SSL added.
    * */
  private def addSSL(connectorConfig: AbstractConfig, builder: Builder) : Builder = {
    val ssl = connectorConfig.getBoolean(CassandraConfigConstants.SSL_ENABLED)
    ssl match {
      case true =>
        logger.info("Setting up SSL context.")
        val sslConfig = SSLConfig(
          trustStorePath = connectorConfig.getString(CassandraConfigConstants.TRUST_STORE_PATH),
          trustStorePass =  connectorConfig.getPassword(CassandraConfigConstants.TRUST_STORE_PASSWD).value,
          keyStorePath = Some(connectorConfig.getString(CassandraConfigConstants.KEY_STORE_PATH)),
          keyStorePass = Some(connectorConfig.getPassword(CassandraConfigConstants.KEY_STORE_PASSWD).value),
          useClientCert = connectorConfig.getBoolean(CassandraConfigConstants.USE_CLIENT_AUTH)
        )

        val context = SSLConfigContext(sslConfig)
        //val cipherSuites: Array[String] = Array("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA")
        val sSLOptions = new JdkSSLOptions.Builder
        //sSLOptions.withCipherSuites(cipherSuites)
        sSLOptions.withSSLContext(context)
        builder.withSSL(sSLOptions.build())
      case false => builder
    }
  }
}

/**
  * <h1>CassandraConnection</h1>
  *
  * Case class to hold a Cassandra cluster and session connection
  * */
case class CassandraConnection(session: Session)
