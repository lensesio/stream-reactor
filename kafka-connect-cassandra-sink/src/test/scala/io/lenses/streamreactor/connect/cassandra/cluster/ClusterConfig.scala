/*
 * Copyright 2017-2026 Lenses.io Ltd
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

import com.datastax.oss.dsbulk.tests.ccm.CCMCluster

case class ClusterConfig(
  numberOfNodes: Array[Int] = Array(1),
  config: Array[String] = Array("write_request_timeout_in_ms:60000", "read_request_timeout_in_ms:60000",
    "range_request_timeout_in_ms:60000", "request_timeout_in_ms:60000"),
  dseConfig:            Array[String]                     = Array("cql_slow_log_options.enabled:false"),
  jvmArgs:              Array[String]                     = Array.empty,
  createOptions:        Array[String]                     = Array.empty,
  ssl:                  Boolean                           = false,
  hostnameVerification: Boolean                           = false,
  auth:                 Boolean                           = false,
  workloads:            Array[Array[CCMCluster.Workload]] = Array.empty,
)
