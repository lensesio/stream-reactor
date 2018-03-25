/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.connect.azure.documentdb.sink

import com.microsoft.azure.documentdb.{DocumentClient, DocumentCollection, RequestOptions}

import scala.util.{Failure, Success, Try}

object CreateDocumentCollectionFn {
  def apply(collection: String, databaseName: String)(implicit documentClient: DocumentClient): DocumentCollection = {
    val coll = new DocumentCollection()
    coll.setId(collection)

    val requestOptions = new RequestOptions()
    Try(documentClient.createCollection(databaseName, coll, requestOptions).getResource) match {
      case Failure(ex) => throw new RuntimeException(s"Could not create collection with id:$collection in database:$databaseName. ${ex.getMessage}", ex)
      case Success(c) => c
    }
  }
}
