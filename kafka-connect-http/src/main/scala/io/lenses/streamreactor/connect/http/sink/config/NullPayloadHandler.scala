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
package io.lenses.streamreactor.connect.http.sink.config

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionError
import org.apache.kafka.common.config.ConfigException

/**
  * Companion object for NullPayloadHandler.
  * Provides factory methods and constants for different null payload handlers.
  */
object NullPayloadHandler {

  val NullPayloadHandlerName   = "null"
  val ErrorPayloadHandlerName  = "error"
  val EmptyPayloadHandlerName  = "empty"
  val CustomPayloadHandlerName = "custom"

  /**
    * Factory method to create a NullPayloadHandler based on the provided name.
    *
    * @param nullPayloadHandlerName the name of the null payload handler
    * @param customSubstitution the custom substitution value (used only for custom handler)
    * @return Either a NullPayloadHandler or a Throwable if the handler name is invalid
    */
  def apply(nullPayloadHandlerName: String, customSubstitution: String): Either[Throwable, NullPayloadHandler] =
    nullPayloadHandlerName.toLowerCase() match {
      case NullPayloadHandlerName   => NullLiteralNullPayloadHandler.asRight
      case ErrorPayloadHandlerName  => ErrorNullPayloadHandler.asRight
      case EmptyPayloadHandlerName  => EmptyStringNullPayloadHandler.asRight
      case CustomPayloadHandlerName => CustomNullPayloadHandler(customSubstitution).asRight
      case _                        => new ConfigException("Invalid null payload handler specified").asLeft
    }
}

/**
  * Trait representing a handler for null payloads.
  */
trait NullPayloadHandler {

  /**
    * Handles a null value and returns either a SubstitutionError or a String.
    *
    * @return Either a SubstitutionError or a String
    */
  def handleNullValue: Either[SubstitutionError, String]

}

/**
  * NullPayloadHandler implementation that returns a SubstitutionError.
  */
object ErrorNullPayloadHandler extends NullPayloadHandler {

  /**
    * Returns a SubstitutionError indicating that a null payload was encountered.
    *
    * @return Left containing a SubstitutionError
    */
  override def handleNullValue: Either[SubstitutionError, String] = SubstitutionError(
    "Templating substitution returned a null payload, and you have configured this to cause an error.",
  ).asLeft
}

/**
  * NullPayloadHandler implementation that returns the string "null".
  */
object NullLiteralNullPayloadHandler extends NullPayloadHandler {

  /**
    * Returns the string "null".
    *
    * @return Right containing the string "null"
    */
  override def handleNullValue: Either[SubstitutionError, String] = "null".asRight
}

/**
  * NullPayloadHandler implementation that returns an empty string.
  */
object EmptyStringNullPayloadHandler extends NullPayloadHandler {

  /**
    * Returns an empty string.
    *
    * @return Right containing an empty string
    */
  override def handleNullValue: Either[SubstitutionError, String] = "".asRight
}

/**
  * NullPayloadHandler implementation that returns a custom value.
  *
  * @param customValue the custom value to return
  */
case class CustomNullPayloadHandler(customValue: String) extends NullPayloadHandler {

  /**
    * Returns the custom value.
    *
    * @return Right containing the custom value
    */
  override def handleNullValue: Either[SubstitutionError, String] = customValue.asRight
}
