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
package io.lenses.streamreactor.connect.datalake.storage

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.azure.core.http.rest.PagedIterable
import com.azure.core.http.rest.PagedResponse
import com.azure.storage.file.datalake.models.PathItem
import io.lenses.streamreactor.connect.datalake.storage.adaptors.DatalakePageIterableAdaptor
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.when

import java.util.function
import scala.jdk.CollectionConverters.SeqHasAsJava
class DatalakePageIterableAdaptorTest extends AnyFunSuite with Matchers {

  test("getResults should return an empty sequence for an empty PagedIterable") {
    val firstPageRetriever: function.Supplier[PagedResponse[PathItem]]         = () => null
    val nextPageRetriever:  function.Function[String, PagedResponse[PathItem]] = (_: String) => null
    val emptyPagedIterable = new PagedIterable[PathItem](firstPageRetriever, nextPageRetriever)

    val result = DatalakePageIterableAdaptor.getResults(emptyPagedIterable)
    result shouldBe empty
  }

  test("getResults should convert a non-empty PagedIterable to a Seq") {
    val pathItem1 = mock[PathItem]
    val pathItem2 = mock[PathItem]
    val pathItem3 = mock[PathItem]

    val firstPagedResponse = mock[PagedResponse[PathItem]]
    when(firstPagedResponse.getValue).thenReturn(List(pathItem1, pathItem2, pathItem3).asJava)
    val firstPageRetriever: function.Supplier[PagedResponse[PathItem]]         = () => firstPagedResponse
    val nextPageRetriever:  function.Function[String, PagedResponse[PathItem]] = (_: String) => null

    val pagedIterable = new PagedIterable[PathItem](firstPageRetriever, nextPageRetriever)

    val result = DatalakePageIterableAdaptor.getResults(pagedIterable)
    result should have size 3
    result should contain allOf (pathItem1, pathItem2, pathItem3)
  }
}
