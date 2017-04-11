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

package com.datamountaineer.streamreactor.connect.hazelcast.config

/**
  * Created by tomasfartaria on 10/04/2017.
  */

object HazelCastSinkConfigConstants {
  val CLUSTER_SOURCE_MEMBERS = "connect.hazelcast.source.cluster.members"
  val CLUSTER_SINK_MEMBERS = "connect.hazelcast.sink.cluster.members"
  val CLUSTER_MEMBERS_DOC: String =
    """Address List is the initial list of cluster addresses to which the client will connect.
      |The client uses this list to find an alive node. Although it may be enough to give only one
      |address of a node in the cluster (since all nodes communicate with each other),
      |it is recommended that you give the addresses for all the nodes.""".stripMargin
  val CLUSTER_MEMBERS_DEFAULT = "localhost"

  val SINK_GROUP_NAME = "connect.hazelcast.sink.group.name"
  val SINK_GROUP_NAME_DOC = "The group name of the connector in the target Hazelcast cluster."

  val SINK_GROUP_PASSWORD = "connect.hazelcast.sink.group.password"
  val SINK_GROUP_PASSWORD_DOC: String = """The password for the group name.""".stripMargin
  val SINK_GROUP_PASSWORD_DEFAULT = "dev-pass"

  val PARALLEL_WRITE = "connect.hazelcast.parallel.write"
  val PARALLEL_WRITE_DOC = "All the sink to write in parallel the records received from Kafka on each poll."
  val PARALLEL_WRITE_DEFAULT = false

  val CONNECTION_TIMEOUT = "connect.hazelcast.connection.timeout"
  val CONNECTION_TIMEOUT_DOC: String =
    """
      |Connection timeout is the timeout value in milliseconds for nodes to
      |accept client connection requests.""".stripMargin
  val CONNECTION_TIMEOUT_DEFAULT = 5000

  val CONNECTION_RETRY_ATTEMPTS = "connect.hazelcast.connection.retries"
  val CONNECTION_RETRY_ATTEMPTS_DOC: String = """Number of times a client will retry the connection at startup.""".stripMargin
  val CONNECTION_RETRY_ATTEMPTS_DEFAULT = 2

  val KEEP_ALIVE = "connect.hazelcast.connection.keep.alive"
  val KEEP_ALIVE_DOC: String = """Enables/disables the SO_KEEPALIVE socket option. The default value is true.""".stripMargin
  val KEEP_ALIVE_DEFAULT = true

  val TCP_NO_DELAY = "connect.hazelcast.connection.tcp.no.delay"
  val TCP_NO_DELAY_DOC: String = """Enables/disables the TCP_NODELAY socket option. The default value is true.""".stripMargin
  val TCP_NO_DELAY_DEFAULT = true

  val REUSE_ADDRESS = "connect.hazelcast.connection.reuse.address"
  val REUSE_ADDRESS_DOC: String = """Enables/disables the SO_REUSEADDR socket option. The default value is true.""".stripMargin
  val REUSE_ADDRESS_DEFAULT = true

  val LINGER_SECONDS = "connect.hazelcast.connection.linger.seconds"
  val LINGER_SECONDS_DOC: String =
    """Enables/disables SO_LINGER with the specified linger time in seconds.
      |The default value is 3.""".stripMargin
  val LINGER_SECONDS_DEFAULT = 3

  val BUFFER_SIZE = "connect.hazelcast.connection.buffer.size"
  val BUFFER_SIZE_DOC: String =
    """Sets the SO_SNDBUF and SO_RCVBUF options to the specified value in KB for this Socket.
      |The default value is 32.""".stripMargin
  val BUFFER_SIZE_DEFAULT = 32

  val EXPORT_ROUTE_QUERY = "connect.hazelcast.sink.kcql"
  val EXPORT_ROUTE_QUERY_DOC = "KCQL expression describing field selection and routes."

  val ERROR_POLICY = "connect.hazelcast.sink.error.policy"
  val ERROR_POLICY_DOC: String =
    """Specifies the action to be taken if an error occurs while inserting the data.
      |There are two available options:
      |NOOP - the error is swallowed
      |THROW - the error is allowed to propagate.
      |RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on
      |The error will be logged automatically""".stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = "connect.hazelcast.sink.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES = "connect.hazelcast.max.retries"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val SINK_THREAD_POOL_CONFIG = "connect.hazelcast.sink.threadpool.size"
  val SINK_THREAD_POOL_DOC =
    """The sink inserts all the data concurrently. To fail fast in case of an error, the sink has its own thread pool.
      |Set the value to zero and the threadpool will default to 4* NO_OF_CPUs. Set a value greater than 0
      |and that would be the size of this threadpool.""".stripMargin
  val SINK_THREAD_POOL_DISPLAY = "Thread pool size"
  val SINK_THREAD_POOL_DEFAULT = 0
}
