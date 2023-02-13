/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.config

import cats.Show
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId.defaultConnectorName
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.TASK_INDEX
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionHasher

import java.util

sealed trait ConnectorTaskId {
  val name: String

  def hasDefaultConnectorName: Boolean = name == defaultConnectorName

  def ownsDir(dirPath: String): Boolean = true
}

object DefaultConnectorTaskId extends ConnectorTaskId {
  override val name: String = defaultConnectorName
}

case class InitedConnectorTaskId(name: String, maxTasks: Int, taskNo: Int) extends ConnectorTaskId {

  override def ownsDir(dirPath: String): Boolean =
    PartitionHasher.hash(maxTasks, dirPath) == taskNo
}

object ConnectorTaskId {

  val defaultConnectorName = "MissingConnectorName"

  def fromProps(props: util.Map[String, String]): ConnectorTaskId = {
    val taskIndex     = props.get(TASK_INDEX).split(":")
    val maybeTaskName = Option(props.get("name")).filter(_.trim.nonEmpty)
    val taskName      = maybeTaskName.getOrElse(defaultConnectorName)
    InitedConnectorTaskId(taskName, taskIndex.head.toInt, taskIndex.last.toInt)
  }

  implicit val showConnector: Show[ConnectorTaskId] = Show.show(_.name)

}
