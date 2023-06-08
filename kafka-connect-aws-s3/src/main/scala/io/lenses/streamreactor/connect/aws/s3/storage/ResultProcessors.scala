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
package io.lenses.streamreactor.connect.aws.s3.storage

import software.amazon.awssdk.services.s3.model.S3Object

object ResultProcessors {

  def processObjectsAsFileMeta(
    bucket:    String,
    prefix:    Option[String],
    objectSeq: Seq[S3Object],
  ): Option[ListResponse[FileMetadata]] = {
    val list = objectSeq
      .map(s3O => FileMetadata(s3O.key(), s3O.lastModified()))

    list
      .lastOption
      .map {
        last =>
          ListResponse[FileMetadata](
            bucket,
            prefix,
            list,
            last,
          )
      }

  }

  def processAsKey(
    bucket:    String,
    prefix:    Option[String],
    objectSeq: Seq[S3Object],
  ): Option[ListResponse[String]] =
    objectSeq.lastOption.map {
      last =>
        ListResponse(
          bucket,
          prefix,
          objectSeq.map(_.key()),
          FileMetadata(last.key(), last.lastModified()),
        )
    }
}
