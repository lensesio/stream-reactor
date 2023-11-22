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
package io.lenses.streamreactor.connect.datalake.storage.adaptors

import cats.implicits.catsSyntaxOptionId
import com.azure.core.http.rest.PagedIterable
import com.azure.core.http.rest.PagedResponse
import com.azure.storage.file.datalake.models.PathItem

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IterableHasAsScala

// After implementing this I realised it's not required until we implement the source.

object DatalakeContinuingPageIterableAdaptor {

  def getResults(
    pagedIter:         PagedIterable[PathItem],
    continuationToken: Option[String],
    lastFilename:      Option[String],
    numResults:        Int,
  ): (Option[String], Seq[PathItem]) = {

    @tailrec
    def loop(
      pagedIter:          PagedIterable[PathItem],
      continuationToken:  Option[String],
      lastFilename:       Option[String],
      numResults:         Int,
      accumulatedResults: Seq[PathItem],
    ): (Option[String], Seq[PathItem]) =
      selectPage(pagedIter, continuationToken).headOption match {
        case Some(page) =>
          val pgValue = page.getValue.asScala

          val entries: Seq[PathItem] = pgValue.toSeq
          val existsOnPage =
            lastFilename.map(lastFileName => entries.indexWhere(_.getName == lastFileName)).getOrElse(-1)
          val results = if (existsOnPage > -1) {
            // skip until we get to the one we're interested in
            entries.splitAt(existsOnPage + 1)._2
          } else {
            entries
          }
          val soFar     = results.take(numResults)
          val exhausted = (results.size - numResults) == 0
          val pgToken: Option[String] = if (exhausted) {
            page.getContinuationToken.some
          } else {
            continuationToken
          }

          if (soFar.size < numResults) {
            loop(pagedIter,
                 page.getContinuationToken.some,
                 lastFilename,
                 numResults - soFar.size,
                 accumulatedResults ++ soFar,
            )
          } else {
            (pgToken, accumulatedResults ++ soFar)
          }

        case None =>
          (None, accumulatedResults)
      }

    loop(pagedIter, continuationToken, lastFilename, numResults, Seq.empty[PathItem])
  }

  private def selectPage(
    pagedIter:             PagedIterable[PathItem],
    pageContinuationToken: Option[String],
  ): Iterable[PagedResponse[PathItem]] =
    pageContinuationToken.fold(
      pagedIter.iterableByPage(),
    )(tok => pagedIter.iterableByPage(tok)).asScala

}
