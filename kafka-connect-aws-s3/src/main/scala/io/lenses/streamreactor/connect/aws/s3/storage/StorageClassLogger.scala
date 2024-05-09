/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.storage

import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import software.amazon.awssdk.services.s3.model.StorageClass

class StorageClassLogger(connectorTaskId: ConnectorTaskId) extends LazyLogging {

  def log(bucket: String, path: String, storageClass: String, operation: String): Unit = {
    val problematicStorageClasses = Set(
      StorageClass.GLACIER,
      StorageClass.GLACIER_IR,
      StorageClass.SNOW,
    ).map(_.name())
    logger.debug(
      s"[${connectorTaskId.show}] Storage class for object '$path' in bucket '$bucket' during operation '$operation': $storageClass",
    )
    if (problematicStorageClasses.contains(storageClass)) {
      logger.error(s"GLACIER storage class found for file $path!!!!")
    }
  }

}
