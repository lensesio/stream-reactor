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
package io.lenses.streamreactor.connect.gcp.storage

import cats.implicits.catsSyntaxOptionId
import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import org.mockito.MockitoSugar

import java.time.OffsetDateTime
import scala.jdk.CollectionConverters.IterableHasAsJava

object SamplePages extends MockitoSugar {

  private var nextPage: Option[Page[Blob]] = Option.empty
  val pages: Seq[Page[Blob]] = (0 to 9).reverse.map {
    page: Int =>
      val results = (0 to 9).map(mockBlob(page, _))

      val thisPage = mockBlobPage(results)
      nextPage = thisPage.some
      thisPage
  }.reverse

  def emptyPage = {
    val emptyPg = mock[Page[Blob]]
    when(emptyPg.getValues).thenReturn(Iterable.empty[Blob].asJava)
    when(emptyPg.getNextPage).thenReturn(null)
    emptyPg
  }

  def mockBlobPage(results: IndexedSeq[Blob]): Page[Blob] = {
    val thisPage: Page[Blob] = mock[Page[Blob]]
    when(thisPage.getValues).thenReturn(results.asJava)
    when(thisPage.getNextPage).thenReturn(nextPage.orNull)
    thisPage
  }

  def mockBlob(page: Int, no: Int): Blob = {
    val blob: Blob = mock[Blob]
    val blobName = s"${(page * 10) + no}.txt"
    when(blob.getCreateTimeOffsetDateTime).thenReturn(OffsetDateTime.now())
    when(blob.getName).thenReturn(blobName)
    blob
  }
}
