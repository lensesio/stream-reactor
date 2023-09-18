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
package io.lenses.streamreactor.connect.aws.s3.config.processors

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._

import scala.collection.MapView
import scala.collection.immutable.ListMap

/**
  * For consistency of configuration, some properties are deprecated in the connector.  To ensure users update their
  * connector configuration, this will fail during connector initialisation advising of the errors and how to update the
  * properties.  This will be removed in a future release.
  */
class DeprecationConfigDefProcessor extends ConfigDefProcessor with LazyLogging {

  private val deprecatedProps: Map[String, String] = ListMap(
    DEP_AUTH_MODE                   -> AUTH_MODE,
    DEP_AWS_ACCESS_KEY              -> AWS_ACCESS_KEY,
    DEP_AWS_SECRET_KEY              -> AWS_SECRET_KEY,
    DEP_ENABLE_VIRTUAL_HOST_BUCKETS -> ENABLE_VIRTUAL_HOST_BUCKETS,
    DEP_CUSTOM_ENDPOINT             -> CUSTOM_ENDPOINT,
  )

  override def process(input: Map[String, Any]): Either[Exception, Map[String, Any]] = {
    val inputKeys = input.keys.toSet
    val failProps = deprecatedProps.view.filterKeys(inputKeys.contains)
    Either.cond(
      failProps.isEmpty,
      input,
      createError(failProps),
    )

  }

  private def createError(failProps: MapView[String, String]): IllegalArgumentException = {
    val keyPrintOut          = failProps.keys.map(k => s"`$k`").mkString(", ")
    val detailedInstructions = failProps.map { case (k, v) => s"Change `$k` to `$v`." }.mkString(" ")
    new IllegalArgumentException(
      s"The following properties have been deprecated: $keyPrintOut. Please change to using the keys prefixed by `connect.s3`. $detailedInstructions",
    )
  }
}
