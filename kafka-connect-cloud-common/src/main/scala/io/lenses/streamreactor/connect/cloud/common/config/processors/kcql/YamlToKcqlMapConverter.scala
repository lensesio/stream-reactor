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
package io.lenses.streamreactor.connect.cloud.common.config.processors.kcql

import cats.implicits.catsSyntaxEitherId

import java.util
import scala.jdk.CollectionConverters.MapHasAsScala

object YamlToKcqlMapConverter {

  def convert(value: util.Map[_, _]): Either[Throwable, Map[KcqlProp, String]] = {
    val mapped = value.asScala.map {
      case (k: String, v: Int) => (KcqlProp.withName(k), String.valueOf(v)).asRight
      case (k: String, v: Long) => (KcqlProp.withName(k), String.valueOf(v)).asRight
      case (k: String, v: String) => (KcqlProp.withName(k), v).asRight
      case other => new IllegalArgumentException("Invalid value for other " + other).asLeft
    }
    val (l, r) = mapped.partitionMap(identity)
    if (l.nonEmpty) {
      l.head.asLeft
    } else {
      r.toMap.asRight
    }
  }
}
