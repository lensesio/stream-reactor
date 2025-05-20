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
package io.lenses.streamreactor.connect.azure.documentdb

import io.lenses.streamreactor.connect.azure.documentdb.config.DocumentDbSinkSettings
import com.microsoft.azure.documentdb.ConnectionPolicy
import com.microsoft.azure.documentdb.DocumentClient
import org.apache.http.HttpHost

/**
  * Creates an instance of Azure DocumentClient class
  */
object DocumentClientProvider {
  def get(settings: DocumentDbSinkSettings): DocumentClient = {
    val policy = ConnectionPolicy.GetDefault()
    settings.proxy.foreach(proxy => policy.setProxy(HttpHost.create(proxy)))

    new DocumentClient(settings.endpoint, settings.masterKey, policy, settings.consistency)
  }
}
