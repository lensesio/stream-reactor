/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.config

import cats.Show
import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import io.lenses.streamreactor.connect.cloud.common.source.config.distribution.PartitionHasher

case class ConnectorTaskId(name: String, maxTasks: Int, taskNo: Int) {
  def ownsDir(dirPath: String): Boolean =
    if (maxTasks == 1) true
    else PartitionHasher.hash(maxTasks, dirPath) == taskNo
}

trait TaskIndexKey extends WithConnectorPrefix {

  val TASK_INDEX: String = s"$connectorPrefix.task.index"

}

object ConnectorTaskId {

  implicit val showConnector: Show[ConnectorTaskId] = Show.show { c =>
    s"${c.name} - ${c.taskNo + 1} of ${c.maxTasks}"
  }

}

class ConnectorTaskIdCreator(val connectorPrefix: String) extends TaskIndexKey {
  def fromProps(props: Map[String, String]): Either[Throwable, ConnectorTaskId] = {
    for {
      taskIndexString <- props.get(TASK_INDEX).toRight(s"Missing $TASK_INDEX")
      taskIndex        = taskIndexString.split(":")
      _               <- if (taskIndex.size != 2) Left(s"Invalid $TASK_INDEX. Expecting TaskNumber:MaxTask format.") else Right(())
      maxTasks <- taskIndex(1).toIntOption.toRight(
        s"Invalid $TASK_INDEX. Expecting an integer but found:${taskIndex(1)}",
      )
      _ <- if (maxTasks <= 0) Left(s"Invalid $TASK_INDEX. Expecting a positive integer but found:${taskIndex(1)}")
      else Right(())
      taskNumber <- taskIndex(0).toIntOption.toRight(
        s"Invalid $TASK_INDEX. Expecting an integer but found:${taskIndex(0)}",
      )
      _ <- if (taskNumber < 0) Left(s"Invalid $TASK_INDEX. Expecting a positive integer but found:${taskIndex(0)}")
      else Right(())
      maybeTaskName <- props.get("name").filter(_.trim.nonEmpty).toRight("Missing connector name")
    } yield ConnectorTaskId(maybeTaskName, maxTasks, taskNumber)
  }.leftMap(new IllegalArgumentException(_))

}
