package com.datamountaineer.streamreactor.connect

import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.{Cluster, Session}

/**
  * Set up a Casssandra connection
  * */

object CassandraConnection extends Logging {
  def apply(contactPoints: String, port: Int , keySpace: String) = {
    log.info(s"Attempting to connect to Cassandra cluster at $contactPoints and create keyspace $keySpace")
    val cluster = Cluster
      .builder()
      .addContactPoints(contactPoints)
      .withPort(port)
      //.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
      .build()
      new CassandraConnection(cluster=cluster, session = cluster.connect(keySpace))
    }
}

case class CassandraConnection(cluster: Cluster, session: Session)