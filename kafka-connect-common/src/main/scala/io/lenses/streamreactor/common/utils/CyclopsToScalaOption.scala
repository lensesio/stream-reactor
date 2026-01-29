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
package io.lenses.streamreactor.common.utils

import cyclops.control.{ Option => CyclopsOption }

import scala.jdk.OptionConverters.RichOption
import scala.jdk.OptionConverters.RichOptional
import scala.{ Option => ScalaOption }

/**
  * Utility object for converting Cyclops Option to Scala Option.
  *
  * This object provides a method to convert an instance of Cyclops Option to a Scala Option.
  */
object CyclopsToScalaOption {

  /**
    * Converts a Cyclops Option to a Scala Option.
    *
    * This method converts an instance of Cyclops Option to a Scala Option.
    *
    * @tparam M the type of the value contained in the Option
    * @param cyclopsOption the Cyclops Option to convert
    * @return the converted Scala Option
    */
  def convertToScalaOption[M](cyclopsOption: CyclopsOption[M]): ScalaOption[M] =
    cyclopsOption.toOptional.toScala

  /**
    * Converts a Scala Option to a Cyclops Option.
    *
    * This method converts an instance of Scala Option to a Cyclops Option.
    *
    * @tparam M the type of the value contained in the Option
    * @param scalaOption the Scala Option to convert
    * @return the converted Cyclops Option
    */
  def convertToCyclopsOption[M](scalaOption: Option[M]): CyclopsOption[M] =
    CyclopsOption.fromOptional(scalaOption.toJava)

}
