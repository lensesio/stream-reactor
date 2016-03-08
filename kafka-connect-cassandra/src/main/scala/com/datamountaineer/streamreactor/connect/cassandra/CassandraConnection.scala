package com.datamountaineer.streamreactor.connect.cassandra

import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.scalalogging.slf4j.StrictLogging

/**
  * Set up a Casssandra connection
  * */

object CassandraConnection extends StrictLogging {
  def apply(contactPoints: String, port: Int , keySpace: String) = {
    logger.info(s"Attempting to connect to Cassandra cluster at $contactPoints and create keyspace $keySpace")

    val cluster = Cluster
      .builder()
      .addContactPoints(contactPoints)
      .withPort(port)
      .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
      .build()
      new CassandraConnection(cluster=cluster, session = cluster.connect(keySpace))
    }
}


/**
  * <h1>CassandraConnection</h1>
  *
  * Case class to hold a Cassandra cluster and session connection
  * */
case class CassandraConnection(cluster: Cluster, session: Session)