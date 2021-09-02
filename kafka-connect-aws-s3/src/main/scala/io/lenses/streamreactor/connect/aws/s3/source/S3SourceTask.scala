/*
 * Copyright 2020 Lenses.io
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

import com.datamountaineer.streamreactor.common.utils.JarManifest
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.auth.AuthResources
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigDefBuilder
import io.lenses.streamreactor.connect.aws.s3.model.location.{RemoteS3PathLocationWithLine, RemoteS3RootLocation}
import io.lenses.streamreactor.connect.aws.s3.source.SourceRecordConverter.convertToSourceRecordList
import io.lenses.streamreactor.connect.aws.s3.source.config.S3SourceConfig
import io.lenses.streamreactor.connect.aws.s3.storage.{AwsS3StorageInterface, JCloudsStorageInterface, S3StorageInterface}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import java.util
import scala.collection.JavaConverters._
import scala.util.Try

class S3SourceTask extends SourceTask with LazyLogging {

  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  private var readerManagers: Seq[S3BucketReaderManager] = _

  override def version(): String = manifest.version()

  private def propsFromContext(props: util.Map[String, String]): util.Map[String, String] = {
    Option(context)
      .flatMap(c => Option(c.configs()))
      .filter(!_.isEmpty)
      .getOrElse(props)
  }

  /**
    * Start sets up readers for every configured connection in the properties
    * */
  override def start(props: util.Map[String, String]): Unit = {

    logger.debug(s"Received call to S3SourceTask.start with ${props.size()} properties")

    val contextFn : RemoteS3RootLocation => Option[RemoteS3PathLocationWithLine] = new ContextReader(() => context).getCurrentOffset

    val eitherErrOrReaderMan = for {
      config <- Try(S3SourceConfig(S3ConfigDefBuilder(getSourceName(props), propsFromContext(props)))).toEither
      authResources = new AuthResources(config.s3Config)
      jCloudsAuth <- authResources.jClouds
      awsAuth <- authResources.aws
      storageInterface <- Try(new S3StorageInterface(getSourceName(props).getOrElse("EmptySourceName"), awsAuth, jCloudsAuth)).toEither
      readerManagers = config.bucketOptions.map(
        bOpts =>
          new S3BucketReaderManager(
            bOpts.recordsLimit,
            bOpts.format,
            contextFn(bOpts.sourceBucketAndPrefix),
            new S3SourceFileQueue(
              bOpts.sourceBucketAndPrefix,
              bOpts.filesLimit,
              new S3SourceLister(bOpts.format.format)(storageInterface),
            ),
            new ResultReader(bOpts.sourceBucketAndPrefix.prefixOrDefault(), bOpts.targetTopic),
          )(
            storageInterface
          )
      )
    } yield readerManagers

    eitherErrOrReaderMan match {
      case Left(err: Throwable) => throw err
      case Left(err: String) => throw new IllegalStateException(err)
      case Right(readerMan) => readerManagers = readerMan
    }
  }

  override def stop(): Unit = {
    logger.debug(s"Received call to S3SinkTask.stop")

    readerManagers.foreach(_.close())
  }

  override def poll(): util.List[SourceRecord] = {
    readerManagers
      .flatMap(_.poll())
      .flatMap(convertToSourceRecordList)
      .asJava
  }

  private def getSourceName(props: util.Map[String, String]) = {
    Option(props.get("name")).filter(_.trim.nonEmpty)
  }
}
