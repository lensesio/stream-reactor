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
package io.lenses.streamreactor.connect.gcp.storage.auth

import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import io.lenses.streamreactor.connect.cloud.common.auth.ClientCreator
import io.lenses.streamreactor.connect.gcp.common.auth.GCPConnectionConfig
import io.lenses.streamreactor.connect.gcp.common.auth.GCPServiceBuilderConfigurer

import scala.util.Try

object GCPStorageClientCreator extends ClientCreator[GCPConnectionConfig, Storage] {

  def make(config: GCPConnectionConfig): Either[Throwable, Storage] =
    Try {
      val builder: StorageOptions.Builder = StorageOptions.newBuilder()

      GCPServiceBuilderConfigurer.configure[
        Storage,
        StorageOptions,
        StorageOptions.Builder,
      ](config, builder).build().getService
    }.toEither

}
