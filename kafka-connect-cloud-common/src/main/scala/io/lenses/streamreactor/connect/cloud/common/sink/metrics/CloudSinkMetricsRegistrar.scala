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
package io.lenses.streamreactor.connect.cloud.common.sink.metrics

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId

import java.lang.management.ManagementFactory
import javax.management.MBeanServer
import javax.management.ObjectName

object CloudSinkMetricsRegistrar {

  private val Domain = "io.lenses.streamreactor.connect.cloud.sink"

  private val MaxRegistrationAttempts = 3

  private def objectName(connectorTaskId: ConnectorTaskId): ObjectName =
    new ObjectName(s"$Domain:type=metrics,name=${connectorTaskId.name},task=${connectorTaskId.taskNo}")

  def register(metrics: CloudSinkMetricsMBean, connectorTaskId: ConnectorTaskId): Unit =
    register(ManagementFactory.getPlatformMBeanServer, metrics, connectorTaskId)

  private[metrics] def register(
    mbs:             MBeanServer,
    metrics:         CloudSinkMetricsMBean,
    connectorTaskId: ConnectorTaskId,
  ): Unit =
    registerWithRetry(mbs, metrics, objectName(connectorTaskId), MaxRegistrationAttempts)

  def unregister(connectorTaskId: ConnectorTaskId): Unit = {
    val mbs: MBeanServer = ManagementFactory.getPlatformMBeanServer
    val name = objectName(connectorTaskId)
    if (mbs.isRegistered(name)) {
      mbs.unregisterMBean(name)
    }
  }

  @annotation.tailrec
  private def registerWithRetry(
    mbs:               MBeanServer,
    metrics:           CloudSinkMetricsMBean,
    name:              ObjectName,
    attemptsRemaining: Int,
  ): Unit =
    try {
      if (mbs.isRegistered(name)) {
        mbs.unregisterMBean(name)
      }
      mbs.registerMBean(metrics, name)
      ()
    } catch {
      case e: javax.management.InstanceAlreadyExistsException =>
        if (attemptsRemaining <= 1) {
          throw new IllegalStateException(
            s"Failed to register MBean $name after $MaxRegistrationAttempts attempts due to concurrent registration",
            e,
          )
        }
        try mbs.unregisterMBean(name)
        catch { case _: javax.management.InstanceNotFoundException => () }
        registerWithRetry(mbs, metrics, name, attemptsRemaining - 1)
    }
}
