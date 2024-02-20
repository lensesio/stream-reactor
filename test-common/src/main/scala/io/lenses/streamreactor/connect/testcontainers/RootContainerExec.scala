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
package io.lenses.streamreactor.connect.testcontainers

import cats.implicits._
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.FrameConsumerResultCallback
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.containers.output.ToStringConsumer

import java.nio.charset.Charset
import scala.util.Try

trait ExecResult {
  val exitCode: Long
}

object ExceptionExecResult {
  def apply(err: Throwable): ExceptionExecResult =
    ExceptionExecResult.apply(err.getMessage, err.some)
  def apply(errMsg: String): ExceptionExecResult =
    ExceptionExecResult.apply(errMsg, Option.empty)
}
case class ExceptionExecResult(msg: String, err: Option[Throwable]) extends ExecResult {
  override val exitCode: Long = -1L
}

case class FailedProcessExecResult(
  exitCode: Long,
  stdOut:   String,
  stdErr:   String,
) extends ExecResult

case class SuccessfulExecResult() extends ExecResult {
  val exitCode = 1
}

trait RootContainerExec {

  private def createCallback(stdoutConsumer: ToStringConsumer, stderrConsumer: ToStringConsumer) = {
    val callback = new FrameConsumerResultCallback()
    callback.addConsumer(OutputFrame.OutputType.STDOUT, stdoutConsumer)
    callback.addConsumer(OutputFrame.OutputType.STDERR, stderrConsumer)
    callback
  }

  def rootExecInContainer(
    container:     GenericContainer[_],
    outputCharset: Charset = Charset.defaultCharset(),
    commands:      Seq[String],
  ): ExecResult = {

    val stdoutConsumer = new ToStringConsumer()
    val stderrConsumer = new ToStringConsumer()
    val callback: FrameConsumerResultCallback = createCallback(stdoutConsumer, stderrConsumer)

    for {
      _ <- Either.cond(container.isRunning,
                       (),
                       ExceptionExecResult("execInContainer can only be used while the Container is running"),
      )
      exitCode <- Try {
        val dockerClient = DockerClientFactory.instance().client()
        val execCreateCmdResponse = dockerClient
          .execCreateCmd(container.getContainerId)
          .withUser("root")
          .withAttachStdout(true)
          .withAttachStderr(true)
          .withCmd(commands: _*)
          .exec()
        dockerClient.execStartCmd(execCreateCmdResponse.getId).exec(callback).awaitCompletion()
        dockerClient.inspectExecCmd(execCreateCmdResponse.getId).exec().getExitCodeLong
      }.toEither.leftMap {
        err => ExceptionExecResult(err)
      }
    } yield {
      exitCode.toLong match {
        case 0L => SuccessfulExecResult()
        case _ =>
          FailedProcessExecResult(
            exitCode,
            stdoutConsumer.toString(outputCharset),
            stderrConsumer.toString(outputCharset),
          )
      }
    }

  }.merge

}
