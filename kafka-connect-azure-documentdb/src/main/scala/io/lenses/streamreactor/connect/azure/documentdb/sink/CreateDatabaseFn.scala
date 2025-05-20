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
package io.lenses.streamreactor.connect.azure.documentdb.sink

import com.microsoft.azure.documentdb.Database
import com.microsoft.azure.documentdb.DocumentClient

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object CreateDatabaseFn {
  def apply(databaseName: String)(implicit documentClient: DocumentClient): Database = {
    val db = new Database()
    db.setId(databaseName)
    Try(documentClient.createDatabase(db, null).getResource) match {
      case Failure(ex) => throw new RuntimeException(s"Could not create database [$databaseName]. ${ex.getMessage}", ex)
      case Success(d)  => d
    }
  }
}
