/*
 * Copyright 2017-2025 Lenses.io Ltd
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

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.AUTH_MODE
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.AWS_ACCESS_KEY
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.AWS_SECRET_KEY
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.CONNECTOR_PREFIX
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.CUSTOM_ENDPOINT
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.ENABLE_VIRTUAL_HOST_BUCKETS
import io.lenses.streamreactor.connect.aws.s3.config.processors.kcql.DeprecationConfigDefProcessor.DEP_AUTH_MODE
import io.lenses.streamreactor.connect.aws.s3.config.processors.kcql.DeprecationConfigDefProcessor.DEP_AWS_ACCESS_KEY
import io.lenses.streamreactor.connect.aws.s3.config.processors.kcql.DeprecationConfigDefProcessor.DEP_AWS_SECRET_KEY
import io.lenses.streamreactor.connect.aws.s3.config.processors.kcql.DeprecationConfigDefProcessor.DEP_CUSTOM_ENDPOINT
import io.lenses.streamreactor.connect.aws.s3.config.processors.kcql.DeprecationConfigDefProcessor.DEP_ENABLE_VIRTUAL_HOST_BUCKETS
import io.lenses.streamreactor.connect.aws.s3.config.processors.kcql.DeprecationConfigDefProcessor.DEP_SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS
import io.lenses.streamreactor.connect.aws.s3.config.processors.kcql.DeprecationConfigDefProcessor.DEP_SOURCE_PARTITION_SEARCH_MODE
import io.lenses.streamreactor.connect.aws.s3.config.processors.kcql.DeprecationConfigDefProcessor.DEP_SOURCE_PARTITION_SEARCH_RECURSE_LEVELS
import io.lenses.streamreactor.connect.cloud.common.config.processors.ConfigDefProcessor
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceSettingsKeys

import scala.collection.MapView
import scala.collection.immutable.ListMap

object DeprecationConfigDefProcessor {

  // Deprecated and will be removed in future
  val DEP_AWS_ACCESS_KEY:              String = "aws.access.key"
  val DEP_AWS_SECRET_KEY:              String = "aws.secret.key"
  val DEP_AUTH_MODE:                   String = "aws.auth.mode"
  val DEP_CUSTOM_ENDPOINT:             String = "aws.custom.endpoint"
  val DEP_ENABLE_VIRTUAL_HOST_BUCKETS: String = "aws.vhost.bucket"

  // Deprecated due to incorrect implementation - all the other properties have "source" in the property key
  val DEP_SOURCE_PARTITION_SEARCH_RECURSE_LEVELS:  String = "connect.s3.partition.search.recurse.levels"
  val DEP_SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS: String = s"connect.s3.partition.search.interval"
  val DEP_SOURCE_PARTITION_SEARCH_MODE:            String = s"connect.s3.partition.search.continuous"

}

/**
  * For consistency of configuration, some properties are deprecated in the connector.  To ensure users update their
  * connector configuration, this will fail during connector initialisation advising of the errors and how to update the
  * properties.  This will be removed in a future release.
  */
class DeprecationConfigDefProcessor
    extends ConfigDefProcessor
    with LazyLogging
    with CloudSourceSettingsKeys
    with WithConnectorPrefix {

  private val deprecatedProps: Map[String, String] = ListMap(
    DEP_AUTH_MODE                               -> AUTH_MODE,
    DEP_AWS_ACCESS_KEY                          -> AWS_ACCESS_KEY,
    DEP_AWS_SECRET_KEY                          -> AWS_SECRET_KEY,
    DEP_ENABLE_VIRTUAL_HOST_BUCKETS             -> ENABLE_VIRTUAL_HOST_BUCKETS,
    DEP_CUSTOM_ENDPOINT                         -> CUSTOM_ENDPOINT,
    DEP_SOURCE_PARTITION_SEARCH_RECURSE_LEVELS  -> SOURCE_PARTITION_SEARCH_RECURSE_LEVELS,
    DEP_SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS,
    DEP_SOURCE_PARTITION_SEARCH_MODE            -> SOURCE_PARTITION_SEARCH_MODE,
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

  override def connectorPrefix: String = CONNECTOR_PREFIX
}
