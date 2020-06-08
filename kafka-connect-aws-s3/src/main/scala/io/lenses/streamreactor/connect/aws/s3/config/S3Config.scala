
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
import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.config.PartitionDisplay.KeysAndValues
import io.lenses.streamreactor.connect.aws.s3.sink._
import io.lenses.streamreactor.connect.aws.s3.{BucketAndPrefix, PartitionField}

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

sealed trait FormatOptions extends EnumEntry

object FormatOptions extends Enum[FormatOptions] {

  val values: immutable.IndexedSeq[FormatOptions] = findValues

  /** CSV Options */
  case object WithHeaders extends FormatOptions

  /** Byte Options */
  case object KeyAndValueWithSizes extends FormatOptions
  case object KeyWithSize extends FormatOptions
  case object ValueWithSize extends FormatOptions
  case object KeyOnly extends FormatOptions
  case object ValueOnly extends FormatOptions

}


case object FormatSelection {

  def apply(formatAsString: String): FormatSelection = {
    val withoutTicks = formatAsString.replace("`", "")
    val split = withoutTicks.split("_")

    val formatOptions: Set[FormatOptions] = if (split.size > 1) {
      split.splitAt(1)._2.flatMap(FormatOptions.withNameInsensitiveOption(_)).toSet
    } else {
      Set.empty
    }

    FormatSelection(Format
      .withNameInsensitiveOption(split(0))
      .getOrElse(throw new IllegalArgumentException(s"Unsupported format - $formatAsString")), formatOptions)
  }

}

case class FormatSelection(
                            format: Format,
                            formatOptions: Set[FormatOptions] = Set.empty
                          )

sealed trait Format extends EnumEntry

object Format extends Enum[Format] {

  val values: immutable.IndexedSeq[Format] = findValues

  case object Json extends Format

  case object Avro extends Format

  case object Parquet extends Format

  case object Text extends Format

  case object Csv extends Format

  case object Bytes extends Format

}

sealed trait PartitionDisplay extends EnumEntry

object PartitionDisplay extends Enum[PartitionDisplay] {
  val values: immutable.IndexedSeq[PartitionDisplay] = findValues

  case object KeysAndValues extends PartitionDisplay

  case object Values extends PartitionDisplay

}

sealed trait BytesWriteMode extends EnumEntry

object BytesWriteMode extends Enum[BytesWriteMode] {
  val values: immutable.IndexedSeq[BytesWriteMode] = findValues

  case object KeyAndValueWithSizes extends BytesWriteMode

  case object KeyWithSize extends BytesWriteMode

  case object ValueWithSize extends BytesWriteMode

  case object KeyOnly extends BytesWriteMode

  case object ValueOnly extends BytesWriteMode

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

      val flushInterval = Option(kcql.getWithFlushInterval).filter(_ > 0).map(_.seconds)
      val flushCount = Option(kcql.getWithFlushCount).filter(_ > 0)


      val formatSelection: FormatSelection = Option(kcql.getStoredAs) match {
        case Some(format: String) => FormatSelection(format)
        case None => FormatSelection(Json, Set.empty)
      }

      val partitionSelection = PartitionSelection(kcql)
      val namingStrategy = partitionSelection match {
        case Some(partSel) => new PartitionedS3FileNamingStrategy(formatSelection, partSel)
        case None => new HierarchicalS3FileNamingStrategy(formatSelection)
      }

      val flushSize = Option(kcql.getWithFlushSize).filter(_ > 0)

      // we must have at least one way of committing files
      val finalFlushSize = Some(flushSize.fold(1000L * 1000 * 128)(identity)) //if (flushSize.isEmpty /*&& flushInterval.isEmpty && flushCount.isEmpty*/) Some(1000L * 1000 * 128) else flushSize

      BucketOptions(
        kcql.getSource,
        BucketAndPrefix(kcql.getTarget),
        formatSelection = formatSelection,
        fileNamingStrategy = namingStrategy,
        partitionSelection = partitionSelection,
        commitPolicy = DefaultCommitPolicy(
          fileSize = finalFlushSize,
          interval = flushInterval,
          recordCount = flushCount
        )
      )
    }

  }

}

case object PartitionSelection {

  def apply(kcql: Kcql): Option[PartitionSelection] = {
    val partitions = Option(kcql.getPartitionBy).map(_.asScala).getOrElse(Nil).map(name => PartitionField(name)).toVector
    if (partitions.isEmpty) None else partitionDisplayFromKcql(kcql, partitions)
  }

  private def partitionDisplayFromKcql(kcql: Kcql, partitions: Vector[PartitionField]): Option[PartitionSelection] = {
    val partitionDisplay = Option(kcql.getWithPartitioner).fold[PartitionDisplay](KeysAndValues) {
      PartitionDisplay
        .withNameInsensitiveOption(_)
        .getOrElse(KeysAndValues)
    }
    Some(PartitionSelection(partitions, partitionDisplay))
  }
}

case class PartitionSelection(
                               partitions: Seq[PartitionField],
                               partitionDisplay: PartitionDisplay = PartitionDisplay.Values
                             )

case class BucketOptions(
                          sourceTopic: String,
                          bucketAndPrefix: BucketAndPrefix,
                          formatSelection: FormatSelection,
                          fileNamingStrategy: S3FileNamingStrategy,
                          partitionSelection: Option[PartitionSelection] = None,
                          commitPolicy: CommitPolicy = DefaultCommitPolicy(Some(1000 * 1000 * 128), None, None),
                        )
