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

package io.lenses.streamreactor.connect.aws.s3.config.processors.kcql

import com.datamountaineer.kcql.Kcql
import enumeratum._

import scala.collection.immutable
import scala.jdk.CollectionConverters.asScalaIteratorConverter

/**
  * KcqlProp is an enum that represents a Kcql Property
  *
  * @param entryName    the string representation of the key, for example to use in a yaml document.
  * @param kcqlToString a function that optionally takes a String value from the Kcql object
  */
sealed abstract class KcqlProp(override val entryName: String, val kcqlToString: Kcql => Option[String]) extends EnumEntry

object KcqlProp extends Enum[KcqlProp] {

  override val values: immutable.IndexedSeq[KcqlProp] = findValues

  case object Source extends KcqlProp("source", k => fromString(k.getSource))

  case object Target extends KcqlProp("target", k => fromString(k.getTarget))

  case object Format extends KcqlProp("format", k => fromString(k.getStoredAs).map(_.replace("`", "")))

  case object Partitioner extends KcqlProp("partitioner", k => fromString(k.getWithPartitioner))

  case object Partitions extends KcqlProp("partitions", k => {
    val filtered: Seq[String] = k.getPartitionBy.asScala.toSeq.filter(_.nonEmpty)
    filtered.size match {
      case 0 => Option.empty[String]
      case _ => Some(filtered.mkString(","))
    }
  })

  case object FlushSize extends KcqlProp("flush_size", k => fromLong(k.getWithFlushSize))

  case object FlushInterval extends KcqlProp("flush_interval", k => fromLong(k.getWithFlushInterval))

  case object FlushCount extends KcqlProp("flush_count", k => fromLong(k.getWithFlushCount))

  private def fromString(source: String): Option[String] = Option(source).filter(_.nonEmpty)

  private def fromLong(source: Long): Option[String] = Option(String.valueOf(source)).filterNot(_ == "0")

}
