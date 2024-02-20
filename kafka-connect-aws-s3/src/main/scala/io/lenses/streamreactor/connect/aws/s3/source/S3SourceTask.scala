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
package io.lenses.streamreactor.connect.aws.s3.source

import cats.effect.FiberIO
import cats.effect.IO
import cats.effect.Ref
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifest
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.source.state.S3SourceState
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.SourceContextReader
import io.lenses.streamreactor.connect.cloud.common.source.state.CloudSourceTaskState
import io.lenses.streamreactor.connect.cloud.common.utils.MapUtils
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._
class S3SourceTask extends SourceTask with LazyLogging {

  implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator
  private val contextOffsetFn: CloudLocation => Option[CloudLocation] =
    SourceContextReader.getCurrentOffset(() => context)

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  @volatile
  private var s3SourceTaskState: Option[CloudSourceTaskState] = None

  @volatile
  private var cancelledRef: Option[Ref[IO, Boolean]] = None

  private var partitionDiscoveryLoop: Option[FiberIO[Unit]] = None

  override def version(): String = manifest.version()

  /**
    * Start sets up readers for every configured connection in the properties
    */
  override def start(props: util.Map[String, String]): Unit = {

    printAsciiHeader(manifest, "/aws-s3-source-ascii.txt")

    logger.debug(s"Received call to S3SourceTask.start with ${props.size()} properties")

    val contextProperties: Map[String, String] =
      Option(context).flatMap(c => Option(c.configs()).map(_.asScala.toMap)).getOrElse(Map.empty)
    val mergedProperties: Map[String, String] = MapUtils.mergeProps(contextProperties, props.asScala.toMap)
    (for {
      result <- S3SourceState.make(mergedProperties, contextOffsetFn)
      fiber  <- result.partitionDiscoveryLoop.start
    } yield {
      s3SourceTaskState      = result.state.some
      cancelledRef           = result.cancelledRef.some
      partitionDiscoveryLoop = fiber.some
    }).unsafeRunSync()
  }

  override def stop(): Unit = {
    logger.info(s"Stopping S3 source task")
    (s3SourceTaskState, cancelledRef, partitionDiscoveryLoop) match {
      case (Some(state), Some(signal), Some(fiber)) => stopInternal(state, signal, fiber)
      case _                                        => logger.info("There is no state to stop.")
    }
    logger.info(s"Stopped S3 source task")
  }

  override def poll(): util.List[SourceRecord] =
    s3SourceTaskState.fold(Collections.emptyList[SourceRecord]()) { state =>
      state.poll().unsafeRunSync().asJava
    }

  private def stopInternal(state: CloudSourceTaskState, signal: Ref[IO, Boolean], fiber: FiberIO[Unit]): Unit = {
    (for {
      _ <- signal.set(true)
      _ <- state.close()
      // Don't join the fiber if it's already been cancelled. It will take potentially the interval time to complete
      // and this can create issues on Connect. The task will be terminated and the resource cleaned up by the GC.
      //_ <- fiber.join.timeout(1.minute).attempt.void
    } yield ()).unsafeRunSync()
    cancelledRef           = None
    partitionDiscoveryLoop = None
    s3SourceTaskState      = None
  }
}
