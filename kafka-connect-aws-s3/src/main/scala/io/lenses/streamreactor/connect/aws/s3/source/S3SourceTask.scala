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
package io.lenses.streamreactor.connect.aws.s3.source

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxOptionId
import com.datamountaineer.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import com.datamountaineer.streamreactor.common.utils.JarManifest
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.state.S3SourceTaskState
import io.lenses.streamreactor.connect.aws.s3.utils.MapUtils
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask

import java.util
import java.util.Collections
import scala.jdk.CollectionConverters._
class S3SourceTask extends SourceTask with LazyLogging {

  private val contextOffsetFn: S3Location => Option[S3Location] =
    SourceContextReader.getCurrentOffset(() => context)

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  @volatile
  private var s3SourceTaskState: Option[S3SourceTaskState] = None

  override def version(): String = manifest.version()

  /**
    * Start sets up readers for every configured connection in the properties
    */
  override def start(props: util.Map[String, String]): Unit = {

    printAsciiHeader(manifest, "/aws-s3-source-ascii.txt")

    logger.debug(s"Received call to S3SourceTask.start with ${props.size()} properties")

    val mergedProperties = MapUtils.mergeProps(Option(context.configs()).map(_.asScala.toMap).getOrElse(Map.empty),
                                               props.asScala.toMap,
    ).asJava
    (for {
      state <- S3SourceTaskState.make(mergedProperties, contextOffsetFn)
      _ <- IO.delay {
        s3SourceTaskState = state.some
      }
    } yield ()).unsafeRunSync()
  }

  override def stop(): Unit = {
    logger.debug(s"Received call to S3SourceTask.stop")
    s3SourceTaskState.foreach(_.close().attempt.void.unsafeRunSync())
  }

  override def poll(): util.List[SourceRecord] =
    s3SourceTaskState.fold(Collections.emptyList[SourceRecord]()) { state =>
      state.poll().unsafeRunSync().asJava
    }

}
