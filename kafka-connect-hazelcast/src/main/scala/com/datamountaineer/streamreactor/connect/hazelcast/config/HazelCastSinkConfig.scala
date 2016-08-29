/**
  * Copyright 2016 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.hazelcast.config

import java.util

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}


/**
  * Created by andrew@datamountaineer.com on 08/08/16. 
  * stream-reactor
  */
object HazelCastSinkConfig {

  val CLUSTER_SOURCE_MEMBERS = "connect.hazelcast.source.cluster.members"
  val CLUSTER_SINK_MEMBERS = "connect.hazelcast.sink.cluster.members"
  val CLUSTER_MEMBERS_DOC =
    """
      |Address List is the initial list of cluster addresses to which the client will connect.
      |The client uses this list to find an alive node. Although it may be enough to give only one
      |address of a node in the cluster (since all nodes communicate with each other),
      |it is recommended that you give the addresses for all the nodes.
    """.stripMargin
  val CLUSTER_MEMBERS_DEFAULT = "localhost"

  val SINK_GROUP_NAME = "connect.hazelcast.sink.group.name"
  val SINK_GROUP_NAME_DOC = "The group name of the connector in the target Hazelcast cluster."


  val SINK_GROUP_PASSWORD = "connect.hazelcast.sink.group.password"
  val SINK_GROUP_PASSWORD_DOC = """The password for the group name.""".stripMargin
  val SINK_GROUP_PASSWORD_DEFAULT = "dev-pass"

  val CONNECTION_TIMEOUT = "connect.hazelcast.connection.timeout"
  val CONNECTION_TIMEOUT_DOC =
    """
      |Connection timeout is the timeout value in milliseconds for nodes to
      |accept client connection requests.""".stripMargin
  val CONNECTION_TIMEOUT_DEFAULT = 5000

  val CONNECTION_RETRY_ATTEMPTS = "connect.hazelcast.connection.retries"
  val CONNECTION_RETRY_ATTEMPTS_DOC = """Number of times a client will retry the connection at startup.""".stripMargin
  val CONNECTION_RETRY_ATTEMPTS_DEFAULT = 2

  val KEEP_ALIVE = "connect.hazelcast.connection.keep.alive"
  val KEEP_ALIVE_DOC = """Enables/disables the SO_KEEPALIVE socket option. The default value is true.""".stripMargin
  val KEEP_ALIVE_DEFAULT = true

  val TCP_NO_DELAY = "connect.hazelcast.connection.tcp.no.delay"
  val TCP_NO_DELAY_DOC = """Enables/disables the TCP_NODELAY socket option. The default value is true.""".stripMargin
  val TCP_NO_DELAY_DEFAULT = true

  val REUSE_ADDRESS = "connect.hazelcast.connection.reuse.address"
  val REUSE_ADDRESS_DOC = """Enables/disables the SO_REUSEADDR socket option. The default value is true.""".stripMargin
  val REUSE_ADDRESS_DEFAULT = true

  val LINGER_SECONDS = "connect.hazelcast.connection.linger.seconds"
  val LINGER_SECONDS_DOC =
    """Enables/disables SO_LINGER with the specified linger time in seconds.
      |The default value is 3.""".stripMargin
  val LINGER_SECONDS_DEFAULT = 3

  val BUFFER_SIZE = "connect.hazelcast.connection.buffer.size"
  val BUFFER_SIZE_DOC =
    """Sets the SO_SNDBUF and SO_RCVBUF options to the specified value in KB for this Socket.
      |The default value is 32.""".stripMargin
  val BUFFER_SIZE_DEFAULT = 32

  val EXPORT_ROUTE_QUERY = "connect.hazelcast.export.route.query"
  val EXPORT_ROUTE_QUERY_DOC =  "KCQL expression describing field selection and routes."

  val ERROR_POLICY = "connect.hazelcast.sink.error.policy"
  val ERROR_POLICY_DOC = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
    "There are two available options: \n" + "NOOP - the error is swallowed \n" +
    "THROW - the error is allowed to propagate. \n" +
    "RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on \n" +
    "The error will be logged automatically"
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = "connect.hazelcast.sink.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES = "connect.hazelcast.max.retires"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val BATCH_SIZE = "connect.hazelcast.sink.batch.size"
  val BATCH_SIZE_DOC = "Per topic the number of sink records to batch together and insert into Hazelcast"
  val BATCH_SIZE_DEFAULT = 1000

  val config: ConfigDef = new ConfigDef()
    .define(CLUSTER_SINK_MEMBERS, Type.LIST, CLUSTER_MEMBERS_DEFAULT, Importance.HIGH, CLUSTER_MEMBERS_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, CLUSTER_SINK_MEMBERS)
    .define(CONNECTION_TIMEOUT, Type.LONG, CONNECTION_TIMEOUT_DEFAULT, Importance.LOW, CONNECTION_TIMEOUT_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, CONNECTION_TIMEOUT)
    .define(CONNECTION_RETRY_ATTEMPTS, Type.INT, CONNECTION_RETRY_ATTEMPTS_DEFAULT, Importance.LOW,
      CONNECTION_RETRY_ATTEMPTS_DOC, "Connection", 3, ConfigDef.Width.MEDIUM, CONNECTION_RETRY_ATTEMPTS)
    .define(KEEP_ALIVE, Type.BOOLEAN, KEEP_ALIVE_DEFAULT, Importance.LOW, KEEP_ALIVE_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, KEEP_ALIVE)
    .define(TCP_NO_DELAY, Type.BOOLEAN, TCP_NO_DELAY_DEFAULT, Importance.LOW, TCP_NO_DELAY_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, TCP_NO_DELAY)
    .define(REUSE_ADDRESS, Type.BOOLEAN, REUSE_ADDRESS_DEFAULT, Importance.LOW, REUSE_ADDRESS_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, REUSE_ADDRESS)
    .define(LINGER_SECONDS, Type.INT, LINGER_SECONDS_DEFAULT, Importance.LOW, LINGER_SECONDS_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM, LINGER_SECONDS)
    .define(BUFFER_SIZE, Type.INT, BUFFER_SIZE_DEFAULT, Importance.LOW, BUFFER_SIZE_DOC,
      "Connection", 7, ConfigDef.Width.MEDIUM, BUFFER_SIZE)
    .define(SINK_GROUP_NAME, Type.STRING, Importance.HIGH, SINK_GROUP_NAME_DOC,
      "Connection", 8, ConfigDef.Width.MEDIUM, SINK_GROUP_NAME)
    .define(SINK_GROUP_PASSWORD, Type.PASSWORD, SINK_GROUP_PASSWORD_DEFAULT, Importance.MEDIUM, SINK_GROUP_PASSWORD_DOC,
      "Connection", 9, ConfigDef.Width.MEDIUM, SINK_GROUP_PASSWORD)
    .define(EXPORT_ROUTE_QUERY, Type.STRING, Importance.HIGH, EXPORT_ROUTE_QUERY,
      "Target", 1, ConfigDef.Width.MEDIUM, EXPORT_ROUTE_QUERY)
    .define(ERROR_POLICY, Type.STRING, ERROR_POLICY_DEFAULT, Importance.HIGH, ERROR_POLICY_DOC,
     "Target", 2, ConfigDef.Width.MEDIUM, ERROR_POLICY)
    .define(ERROR_RETRY_INTERVAL, Type.INT, ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, ERROR_RETRY_INTERVAL_DOC,
      "Target", 3, ConfigDef.Width.MEDIUM, ERROR_RETRY_INTERVAL)
    .define(NBR_OF_RETRIES, Type.INT, NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, NBR_OF_RETRIES_DOC,
      "Target", 4, ConfigDef.Width.MEDIUM, NBR_OF_RETRIES)
    .define(BATCH_SIZE, Type.INT, BATCH_SIZE_DEFAULT, Importance.MEDIUM, BATCH_SIZE_DOC,
      "Target", 5, ConfigDef.Width.MEDIUM, BATCH_SIZE)
}

class HazelCastSinkConfig(props: util.Map[String, String]) extends AbstractConfig(HazelCastSinkConfig.config, props)
