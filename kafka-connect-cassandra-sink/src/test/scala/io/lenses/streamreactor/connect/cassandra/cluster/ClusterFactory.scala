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

import com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Workload
import com.datastax.oss.dsbulk.tests.ccm.DefaultCCMCluster

class ClusterFactory(config: ClusterConfig) {
  import ClusterFactory._

  private val numberOfNodes:        Array[Int]            = config.numberOfNodes
  private val ssl:                  Boolean               = config.ssl
  private val hostnameVerification: Boolean               = config.hostnameVerification
  private val auth:                 Boolean               = config.auth
  private val cassandraConfig:      Map[String, Any]      = toConfigMap(config.config)
  private val dseConfig:            Map[String, Any]      = toConfigMap(config.dseConfig)
  private val jvmArgs:              Set[String]           = toConfigSet(config.jvmArgs)
  private val createOptions:        Set[String]           = toConfigSet(config.createOptions)
  private val workloads:            List[Array[Workload]] = computeWorkloads(config)

  def createCCMClusterBuilder(): DefaultCCMCluster.Builder = {
    val ccmBuilder = DefaultCCMCluster.builder().withNodes(numberOfNodes: _*)
    if (ssl || auth) {
      ccmBuilder.withSSL(hostnameVerification)
    }
    if (auth) {
      ccmBuilder.withAuth()
    }
    for ((k, v) <- cassandraConfig) {
      ccmBuilder.withCassandraConfiguration(k, v)
    }
    for ((k, v) <- dseConfig) {
      ccmBuilder.withDSEConfiguration(k, v)
    }
    for (option <- createOptions) {
      ccmBuilder.withCreateOptions(option)
    }
    for (arg <- jvmArgs) {
      ccmBuilder.withJvmArgs(arg)
    }
    for (i <- workloads.indices) {
      val workload = workloads(i)
      if (workload != null) {
        ccmBuilder.withWorkload(i + 1, workload: _*)
      }
    }
    ccmBuilder
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: ClusterFactory =>
      this.ssl == that.ssl &&
        this.auth == that.auth &&
        java.util.Arrays.equals(this.numberOfNodes, that.numberOfNodes) &&
        this.cassandraConfig == that.cassandraConfig &&
        this.dseConfig == that.dseConfig &&
        this.jvmArgs == that.jvmArgs &&
        this.createOptions == that.createOptions &&
        this.workloads == that.workloads
    case _ => false
  }

  override def hashCode(): Int = {
    var result = java.util.Arrays.hashCode(numberOfNodes)
    result = 31 * result + (if (ssl) 1 else 0)
    result = 31 * result + (if (auth) 1 else 0)
    result = 31 * result + cassandraConfig.hashCode()
    result = 31 * result + dseConfig.hashCode()
    result = 31 * result + jvmArgs.hashCode()
    result = 31 * result + createOptions.hashCode()
    result = 31 * result + workloads.hashCode()
    result
  }
}

object ClusterFactory {
  private def toConfigMap(conf: Array[String]): Map[String, Any] =
    conf.map { aConf =>
      val tokens = aConf.split(":", 2)
      if (tokens.length != 2) throw new IllegalArgumentException(s"Wrong configuration option: $aConf")
      tokens(0) -> tokens(1)
    }.toMap

  private def toConfigSet(config: Array[String]): Set[String] =
    config.toSet

  private def computeWorkloads(config: ClusterConfig): List[Array[Workload]] = {
    val total        = config.numberOfNodes.sum
    val workloads    = Array.fill[Array[Workload]](total)(null)
    val annWorkloads = config.workloads
    for (i <- annWorkloads.indices) {
      workloads(i) = annWorkloads(i)
    }
    workloads.toList
  }
}
