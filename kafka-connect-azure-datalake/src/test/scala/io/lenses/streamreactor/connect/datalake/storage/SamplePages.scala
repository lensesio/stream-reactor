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
package io.lenses.streamreactor.connect.datalake.storage

import com.azure.core.http.HttpHeaders
import com.azure.core.http.HttpRequest
import com.azure.core.http.rest.PagedIterable
import com.azure.core.http.rest.PagedResponse
import com.azure.core.util.IterableStream
import com.azure.storage.file.datalake.models.PathItem
import org.mockito.MockitoSugar.mock

import java.util.function
import java.time.OffsetDateTime
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

object SamplePages {

  case class PageInfo(continuationToken: Option[String], results: Seq[String])

  val pages = (0 to 9).map {
    page =>
      val token = Option.when(page != 9)(s"page_$page->${page + 1}_continuation_token")
      val results = (0 to 9).map {
        no =>
          s"${(page * 10) + no}.txt"
      }
      PageInfo(token, results)
  }

  private def toPathItem(fileName: String): PathItem =
    new PathItem("eTag", OffsetDateTime.now(), 10, "group", false, fileName, "bob", "wrx")

  private def returnPagedResponse(index: Int): PagedResponse[PathItem] = new PagedResponse[PathItem] {

    private val page = pages(index)

    override def getElements: IterableStream[PathItem] =
      new IterableStream[PathItem](page.results.map(name => toPathItem(name)).asJava)

    override def getContinuationToken: String = page.continuationToken.orNull

    override def close(): Unit = {}

    override def getStatusCode: Int = 200

    override def getHeaders: HttpHeaders =
      new HttpHeaders()

    override def getRequest: HttpRequest = mock[HttpRequest]
  }

  private val emptyFirstPageRetriever: function.Supplier[PagedResponse[PathItem]] = () => {
    new PagedResponse[PathItem] {

      override def getElements: IterableStream[PathItem] =
        new IterableStream[PathItem](Iterable.empty[PathItem].asJava)

      override def getContinuationToken: String = null

      override def close(): Unit = {}

      override def getStatusCode: Int = 200

      override def getHeaders: HttpHeaders =
        new HttpHeaders()

      override def getRequest: HttpRequest = mock[HttpRequest]
    }
  }
  private val firstPageRetriever: function.Supplier[PagedResponse[PathItem]] = () => {
    returnPagedResponse(0)
  }
  private val nextPageRetriever: function.Function[String, PagedResponse[PathItem]] = (v1: String) => {
    // find the page
    val nextPgIdx = pages.indexWhere(_.continuationToken.exists(_ == v1)) + 1
    returnPagedResponse(nextPgIdx)
  }

  val pagedIterable      = new PagedIterable[PathItem](firstPageRetriever, nextPageRetriever)
  val emptyPagedIterable = new PagedIterable[PathItem](emptyFirstPageRetriever)

}
