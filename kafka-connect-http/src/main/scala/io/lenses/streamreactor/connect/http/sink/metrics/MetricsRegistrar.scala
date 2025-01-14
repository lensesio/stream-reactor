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
package io.lenses.streamreactor.connect.http.sink.metrics

import java.lang.management.ManagementFactory
import javax.management.ObjectName

object MetricsRegistrar {

  val NameTemplate = "io.lenses.streamreactor.connect.http.sink:type=metrics,name=%s"

  /**
    * Register the metrics MBean exposing the count on 200, 400, 500 and other response codes as well as the http request time percentiles
    * @param metrics
    * @param sinkName
    */
  def registerMetricsMBean(metrics: HttpSinkMetricsMBean, sinkName: String): Unit = {
    val mbs        = ManagementFactory.getPlatformMBeanServer
    val objectName = new ObjectName(s"io.lenses.streamreactor.connect.http.sink:type=metrics,name=$sinkName")
    mbs.registerMBean(metrics, objectName)
    ()
  }
}
