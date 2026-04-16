/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.config
import cats.implicits._
import cats.implicits.catsSyntaxEitherId
import io.lenses.kcql.partitions.Partitions

import java.time.format.DateTimeFormatter
import java.util.TimeZone
import scala.jdk.CollectionConverters.ListHasAsScala

/**
 * The `PartitionField` trait represents a field that can be used for partitioning data.
 * It provides a method to get the name of the field and a flag to indicate if the field supports padding.
 * Different types of partition fields are represented as case classes extending this trait.
 */
sealed trait PartitionField {
  def name(): String

  def supportsPadding: Boolean = false
}

object PartitionField {

  /**
   * Creates a sequence of `PartitionField` instances based on the provided `Partitions` instance.
   *
   * @param partitions The `Partitions` instance containing the partition specifications.
   * @return Either a `Throwable` if an error occurred during the operation, or a `Seq[PartitionField]` containing the created `PartitionField` instances.
   */
  def apply(partitions: Partitions): Either[Throwable, Seq[PartitionField]] =
    partitions.getPartitionBy.asScala.toSeq
      .map {
        spec =>
          val split: Seq[String] = PartitionFieldSplitter.split(spec)
          // if a PartitionSpecifier keyword is found, then use that with the tail of the list - otherwise default to the 'Value' keyword with the entirety of the list.
          val (pSpec, pPath) = PartitionSpecifier.withNameOption(split.head) match {
            case Some(partitionSpecifier) =>
              partitionSpecifier -> split.tail
            case None =>
              PartitionSpecifier.Value -> split
          }

          if (pPath.isEmpty) PartitionField(pSpec) else PartitionField(pSpec, pPath)

      }.sequence.leftMap(new IllegalArgumentException(_))

  /**
   * Creates a `PartitionField` instance for a value partition path.
   *
   * @param valuePartitionPath The sequence of strings representing the value partition path.
   * @return Either a `String` error message if an error occurred during the operation, or a `PartitionField` instance.
   */
  def apply(valuePartitionPath: Seq[String]): Either[String, PartitionField] =
    ValuePartitionField(PartitionNamePath(valuePartitionPath: _*)).asRight

  /**
   * Creates a `PartitionField` instance based on the provided `PartitionSpecifier`.
   *
   * @param partitionSpecifier The `PartitionSpecifier` to use when creating the `PartitionField`.
   * @return Either a `String` error message if an error occurred during the operation, or a `PartitionField` instance.
   */
  def apply(partitionSpecifier: PartitionSpecifier): Either[String, PartitionField] =
    partitionSpecifier match {
      case PartitionSpecifier.Key       => WholeKeyPartitionField.asRight
      case PartitionSpecifier.Topic     => TopicPartitionField.asRight
      case PartitionSpecifier.Partition => PartitionPartitionField.asRight
      case PartitionSpecifier.Header =>
        "cannot partition by Header partition field without path".asLeft
      case PartitionSpecifier.Value =>
        "cannot partition by Value partition field without path".asLeft
      case PartitionSpecifier.Date =>
        "cannot partition by Date partition field without format".asLeft
    }

  /**
   * Creates a `PartitionField` instance based on the provided `PartitionSpecifier` and path.
   *
   * @param partitionSpecifier The `PartitionSpecifier` to use when creating the `PartitionField`.
   * @param path               The sequence of strings representing the path.
   * @return Either a `String` error message if an error occurred during the operation, or a `PartitionField` instance.
   */
  def apply(partitionSpecifier: PartitionSpecifier, path: Seq[String]): Either[String, PartitionField] =
    partitionSpecifier match {
      case PartitionSpecifier.Key    => KeyPartitionField(PartitionNamePath(path: _*)).asRight
      case PartitionSpecifier.Value  => ValuePartitionField(PartitionNamePath(path: _*)).asRight
      case PartitionSpecifier.Header => HeaderPartitionField(PartitionNamePath(path: _*)).asRight
      case PartitionSpecifier.Topic  => "partitioning by topic requires no path".asLeft
      case PartitionSpecifier.Partition =>
        "partitioning by partition requires no path".asLeft
      case PartitionSpecifier.Date =>
        if (path.size == 1) DatePartitionField(path.head).asRight
        else "only one format should be provided for date".asLeft
    }

}

case class HeaderPartitionField(path: PartitionNamePath) extends PartitionField {
  override def name(): String = path.toString

  path.validateProtectedCharacters()
}

case class KeyPartitionField(path: PartitionNamePath) extends PartitionField {
  override def name(): String = path.toString

  path.validateProtectedCharacters()
}

case class ValuePartitionField(path: PartitionNamePath) extends PartitionField {
  override def name(): String = path.toString

  path.validateProtectedCharacters()
}

case object WholeKeyPartitionField extends PartitionField {
  override def name(): String = "key"
}

case object TopicPartitionField extends PartitionField {
  override def name(): String = "topic"
}

case object PartitionPartitionField extends PartitionField {
  override def name(): String = "partition"

  override def supportsPadding: Boolean = true
}

case class DatePartitionField(format: String) extends PartitionField {
  override def name(): String = "date"

  def formatter = DateTimeFormatter.ofPattern(format).withZone(TimeZone.getDefault.toZoneId)
}
