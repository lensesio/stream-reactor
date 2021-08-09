/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink

trait SinkException extends Exception

// Cannot be retried, must be cleaned up
case class CommitException(message: String) extends SinkException {

}

case class BatchCommitException(commitExceptions: Map[MapKey, CommitException]) extends SinkException {

  override def getMessage: String = {
    // TODO add more detail about TPO
    "Multiple exceptions may have been found. Here is a summary.\n" +
      commitExceptions.map(_._2.message).mkString("\n\n")
  }
}

case object ProcessorException {
  def apply(message: String): ProcessorException = {
    ProcessorException(Seq(new IllegalStateException(message)))
  }

  def apply(exception: Throwable): ProcessorException = {
    ProcessorException(Seq(exception))
  }
}

// Can be retried
case class ProcessorException(exceptions: Iterable[Throwable]) extends SinkException {

  override def getMessage: String = {
    "Multiple exceptions may have been found. Here is a summary.\n" +
      exceptions.map(_.getMessage).mkString("\n\n")
  }
}
