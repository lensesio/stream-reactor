package com.datamountaineer.streamreactor.connect

import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.{Cluster, Session}


case class CassandraConnection(cluster: Cluster, session: Session)

object CassandraConnection {
  def apply(contactPoints: String, keySpace: String) = {
   val cluster = Cluster
      .builder()
      .addContactPoints(contactPoints)
      .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
      .build()
    new CassandraConnection(cluster=cluster, session = cluster.connect(keySpace))
  }
}
