
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

package io.lenses.streamreactor.connect.aws.s3.config

import com.datamountaineer.kcql.Kcql
import enumeratum._
import io.lenses.streamreactor.connect.aws.s3.BucketAndPrefix
import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.sink.{CommitPolicy, DefaultCommitPolicy, HierarchicalS3FileNamingStrategy, S3FileNamingStrategy}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.duration._

sealed trait AuthMode extends EnumEntry

object AuthMode extends Enum[AuthMode] {

  val values: immutable.IndexedSeq[AuthMode] = findValues

  case object Credentials extends AuthMode

  case object EC2 extends AuthMode

  case object ECS extends AuthMode

  case object Env extends AuthMode

}


sealed trait Format extends EnumEntry

object Format extends Enum[Format] {

  val values: immutable.IndexedSeq[Format] = findValues

  case object Json extends Format

  case object Avro extends Format

  case object Parquet extends Format

}


object S3Config {

  import S3SinkConfigSettings._

  def apply(props: Map[String, String]): S3Config = S3Config(
    props.getOrElse(AWS_REGION, throw new IllegalArgumentException("No AWS_REGION supplied")),
    props.getOrElse(AWS_ACCESS_KEY, throw new IllegalArgumentException("No AWS_ACCESS_KEY supplied")),
    props.getOrElse(AWS_SECRET_KEY, throw new IllegalArgumentException("No AWS_SECRET_KEY supplied")),
    AuthMode.withNameInsensitive(
      props.getOrElse(AUTH_MODE, throw new IllegalArgumentException("No AUTH_MODE supplied"))
    ),
    props.get(CUSTOM_ENDPOINT),
    props.getOrElse(ENABLE_VIRTUAL_HOST_BUCKETS, "false").toBoolean,
    BucketOptions(props)

  )

}

case class S3Config(
                     region: String,
                     accessKey: String,
                     secretKey: String,
                     authMode: AuthMode,
                     customEndpoint: Option[String] = None,
                     enableVirtualHostBuckets: Boolean = false,

                     bucketOptions: Set[BucketOptions] = Set.empty
                   )

object BucketOptions {
  def apply(props: Map[String, String]): Set[BucketOptions] = {

    val config = S3SinkConfigDefBuilder(props.asJava)

    config.getKCQL.map { kcql: Kcql =>

      val flushSize = Option(kcql.getWithFlushSize).filter(_ > 0)
      val flushInterval = Option(kcql.getWithFlushInterval).filter(_ > 0).map(_.seconds)
      val flushCount = Option(kcql.getWithFlushCount).filter(_ > 0)

      val format: Format = Option(kcql.getStoredAs) match {
        case Some(format: String) =>
          Format
            .withNameInsensitiveOption(format.replace("`", ""))
            .getOrElse(throw new IllegalArgumentException(s"Unsupported format - $format"))
        case None => Json
      }

      // we must have at least one way of committing files
      val finalFlushSize = Some(flushSize.fold(1000L * 1000 * 128)(identity)) //if (flushSize.isEmpty /*&& flushInterval.isEmpty && flushCount.isEmpty*/) Some(1000L * 1000 * 128) else flushSize

      val namingStrategy = new HierarchicalS3FileNamingStrategy(format)

      BucketOptions(
        kcql.getSource,
        BucketAndPrefix(kcql.getTarget),
        format = format,
        fileNamingStrategy = namingStrategy,
        commitPolicy = DefaultCommitPolicy(
          fileSize = finalFlushSize,
          interval = flushInterval,
          recordCount = flushCount
        )
      )
    }

  }
}


case class BucketOptions(
                          sourceTopic: String,
                          bucketAndPrefix: BucketAndPrefix,
                          format: Format,
                          fileNamingStrategy: S3FileNamingStrategy,
                          commitPolicy: CommitPolicy = DefaultCommitPolicy(Some(1000 * 1000 * 128), None, None),
                        )
