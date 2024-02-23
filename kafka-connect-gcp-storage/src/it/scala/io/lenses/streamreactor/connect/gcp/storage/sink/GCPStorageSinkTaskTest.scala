/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.gcp.storage.sink

import com.google.cloud.storage.Storage
import io.lenses.streamreactor.connect.cloud.common.sink.CoreSinkTaskTestCases
import io.lenses.streamreactor.connect.gcp.storage.sink.config.GCPStorageSinkConfig
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageFileMetadata
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageStorageInterface
import io.lenses.streamreactor.connect.gcp.storage.utils.GCPProxyContainerTest

class GCPStorageSinkTaskTest
    extends CoreSinkTaskTestCases[
      GCPStorageFileMetadata,
      GCPStorageStorageInterface,
      GCPStorageSinkConfig,
      Storage,
      GCPStorageSinkTask,
    ](
      "GCPStorageSinkTask",
    )
    with GCPProxyContainerTest {}
