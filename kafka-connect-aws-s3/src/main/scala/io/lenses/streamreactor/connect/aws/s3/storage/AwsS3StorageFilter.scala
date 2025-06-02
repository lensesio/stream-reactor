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
package io.lenses.streamreactor.connect.aws.s3.storage

import software.amazon.awssdk.services.s3.model.ObjectStorageClass
import software.amazon.awssdk.services.s3.model.S3Object

/**
 * Avoids reading objects that are in Glacier or Deep Archive storage classes.
 * These objects are not immediately available and require a restore operation to be performed.
 * This filter is used to avoid reading these objects and failing the task and thus the connector
 */
object AwsS3StorageFilter {

  private val archiveStorageClasses = Set(
    ObjectStorageClass.GLACIER,
    ObjectStorageClass.DEEP_ARCHIVE,
    ObjectStorageClass.GLACIER_IR,
  )
  def filterOut(o: S3Object): Boolean = archiveStorageClasses.contains(o.storageClass)
}
