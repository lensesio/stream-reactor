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

import io.lenses.streamreactor.connect.cassandra.cluster.UseKeyspaceMode.UseKeyspaceMode

/**
 * Scala case class equivalent of the Java SessionConfig annotation interface.
 * Represents session configuration for CCM (Cassandra Cluster Manager) tests.
 */
case class SessionConfig(
  settings:             Array[String]   = Array.empty,
  credentials:          Array[String]   = Array.empty,
  ssl:                  Boolean         = false,
  hostnameVerification: Boolean         = false,
  auth:                 Boolean         = false,
  useKeyspace:          UseKeyspaceMode = UseKeyspaceMode.GENERATE,
  loggedKeyspaceName:   String          = "",
)

/**
 * The strategy to adopt when initializing a CqlSession:
 * whether to create a new random keyspace, a keyspace with a fixed name, or no keyspace.
 */
object UseKeyspaceMode extends Enumeration {
  type UseKeyspaceMode = Value

  val NONE:     Value = Value("NONE")
  val GENERATE: Value = Value("GENERATE")
  val FIXED:    Value = Value("FIXED")
}
