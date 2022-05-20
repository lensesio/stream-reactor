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

import cats.implicits.catsSyntaxEitherId
import com.datamountaineer.kcql.Kcql

import scala.util.Try

/**
  * Converts a Kcql String to a map of properties which can be merged/combined more easier than the
  * Kcql object.
  */
object StringToKcqlMapConverter {

  def convert(kcqlString: String): Either[Throwable, Map[KcqlProp, String]] =
    for {
      kcql <- Try(Kcql.parse(kcqlString)).toEither
      map  <- kcqlToKcqlMap(kcql).asRight
    } yield map

  private def kcqlToKcqlMap(kcql: Kcql): Map[KcqlProp, String] =
    KcqlProp
      .values
      .flatMap(kcqlProp =>
        kcqlProp.kcqlToString(kcql) match {
          case Some("use_profile") => None
          case Some("USE_PROFILE") => None
          case Some(value)         => Some((kcqlProp, value))
          case None                => None
        },
      )
      .toMap
}
