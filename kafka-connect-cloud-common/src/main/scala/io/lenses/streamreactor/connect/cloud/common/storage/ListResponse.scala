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
package io.lenses.streamreactor.connect.cloud.common.storage

trait ListResponse[T, SM <: FileMetadata] {
  def bucket:             String
  def prefix:             Option[String]
  def files:              Seq[T]
  def latestFileMetadata: SM
}

case class ListOfKeysResponse[SM <: FileMetadata](
  bucket:             String,
  prefix:             Option[String],
  files:              Seq[String],
  latestFileMetadata: SM,
) extends ListResponse[String, SM]
case class ListOfMetadataResponse[SM <: FileMetadata](
  bucket:             String,
  prefix:             Option[String],
  files:              Seq[SM],
  latestFileMetadata: SM,
) extends ListResponse[SM, SM]
