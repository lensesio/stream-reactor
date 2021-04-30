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

package io.lenses.streamreactor.connect.aws.s3.model

import com.datamountaineer.kcql.Kcql

import scala.collection.JavaConverters._

sealed trait PartitionField {
  def valuePrefixDisplay(): String
}

object PartitionField {

  def apply(kcql: Kcql): Seq[PartitionField] =
    Option(kcql.getPartitionBy)
      .map(_.asScala)
      .getOrElse(Nil)
      .map(name => {
        val split = name.split("\\.").seq
        PartitionSpecifier.withNameOption(split.head).fold(PartitionField(split))(hd =>
          if (split.tail.isEmpty) PartitionField(hd) else PartitionField(hd, split.tail))
      }).toSeq

  def apply(valuePartitionPath: Seq[String]): PartitionField = {
    ValuePartitionField(PartitionNamePath(valuePartitionPath:_*))
  }

  def apply(partitionSpecifier: PartitionSpecifier): PartitionField = {
    partitionSpecifier match {
      case PartitionSpecifier.Key => WholeKeyPartitionField()
      case PartitionSpecifier.Topic => TopicPartitionField()
      case PartitionSpecifier.Partition => PartitionPartitionField()
      case PartitionSpecifier.Header => throw new IllegalArgumentException("cannot partition by Header partition field without path")
      case PartitionSpecifier.Value => throw new IllegalArgumentException("cannot partition by Value partition field without path")
    }
  }

  def apply(partitionSpecifier: PartitionSpecifier, path: Seq[String]): PartitionField = {
    partitionSpecifier match {
      case PartitionSpecifier.Key => KeyPartitionField(PartitionNamePath(path:_*))
      case PartitionSpecifier.Value => ValuePartitionField(PartitionNamePath(path:_*))
      case PartitionSpecifier.Header => HeaderPartitionField(PartitionNamePath(path:_*))
      case PartitionSpecifier.Topic => throw new IllegalArgumentException("partitioning by topic requires no path")
      case PartitionSpecifier.Partition => throw new IllegalArgumentException("partitioning by partition requires no path")
    }
  }

}

case class HeaderPartitionField(path: PartitionNamePath) extends PartitionField {
  override def valuePrefixDisplay(): String = path.toString

  path.validateProtectedCharacters
}

case class KeyPartitionField(path: PartitionNamePath) extends PartitionField {
  override def valuePrefixDisplay(): String = path.toString

  path.validateProtectedCharacters
}

case class ValuePartitionField(path: PartitionNamePath) extends PartitionField {
  override def valuePrefixDisplay(): String = path.toString

  path.validateProtectedCharacters
}

case class WholeKeyPartitionField() extends PartitionField {
  override def valuePrefixDisplay(): String = "key"
}

case class TopicPartitionField() extends PartitionField {
  override def valuePrefixDisplay(): String = "topic"
}

case class PartitionPartitionField() extends PartitionField {
  override def valuePrefixDisplay(): String = "partition"
}

