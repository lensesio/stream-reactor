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

  val reservedCharacters = Set("/", "\\")
  def validateProtectedCharacters(name: String) =
    require(name != null && name.trim.nonEmpty && reservedCharacters.forall(!name.contains(_)), "name must not be empty and must not contain a slash character")

}

object PartitionField {

  def apply(kcql: Kcql): Seq[PartitionField] =
    Option(kcql.getPartitionBy)
      .map(_.asScala)
      .getOrElse(Nil)
      .map(name => {
        val split = name.split("\\.")
        require(split.size == 1 || split.size == 2, "Invalid partition specification, should contain at most 2 parts")
        if (split.size == 2) {
          PartitionField(split(0), split(1))
        } else {
          PartitionField(split(0))
        }
      }).toSeq

  def apply(keyOrName: String): PartitionField = {
    if (keyOrName.equalsIgnoreCase("_key")) {
      WholeKeyPartitionField()
    } else {
      ValuePartitionField(keyOrName)
    }
  }

  def apply(sourceName: String, name: String): PartitionField = {
    sourceName.toLowerCase match {
      case "_header" => HeaderPartitionField(name)
      case "_key" => KeyPartitionField(name)
      case "_value" => ValuePartitionField(name)
      case _ => throw new IllegalArgumentException(s"Invalid input PartitionSource '$sourceName', should be either '_key', '_value' or '_header'")
    }
  }
}

case class HeaderPartitionField(name: String) extends PartitionField {
  override def valuePrefixDisplay(): String = name

  validateProtectedCharacters(name)
}

case class KeyPartitionField(name: String) extends PartitionField {
  override def valuePrefixDisplay(): String = name

  validateProtectedCharacters(name)
}

case class ValuePartitionField(name: String) extends PartitionField {
  override def valuePrefixDisplay(): String = name

  validateProtectedCharacters(name)
}

case class WholeKeyPartitionField() extends PartitionField {
  override def valuePrefixDisplay(): String = "key"
}


