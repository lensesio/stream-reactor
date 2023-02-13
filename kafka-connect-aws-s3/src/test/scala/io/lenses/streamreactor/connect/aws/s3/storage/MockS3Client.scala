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
package io.lenses.streamreactor.connect.aws.s3.storage

import org.scalatest.Assertions.fail
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CommonPrefix
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable

import scala.jdk.CollectionConverters.SeqHasAsJava

object ContinuationToken {
  def tokenToPage(token: String): Int = Integer.valueOf(token.substring(token.lastIndexOf("-") + 1))

  def pageToToken(page: Int): String = s"continuation-token-$page"

}

case class S3Page(keys: String*) {
  def toMockPage(pageNumber: Int, lastPage: Boolean): MockS3ClientPage =
    MockS3ClientPage(keys.toVector, pageNumber, lastPage)
}
case class MockS3ClientPage(keys: Vector[String], pageNumber: Int, lastPage: Boolean) {

  def matchesPage(token: Option[String]): Boolean =
    token.forall(tok => ContinuationToken.tokenToPage(tok) == pageNumber)
  val continuationToken: Option[String] = Option.when(lastPage)(ContinuationToken.pageToToken(pageNumber))

  val nextContinuationToken: Option[String] = Option.when(!lastPage)(ContinuationToken.pageToToken(pageNumber + 1))

}

class MockS3Client(initPages: S3Page*) extends S3Client {

  private val pages = initPages.zipWithIndex.map {
    case (p, i) => p.toMockPage(i, i == initPages.size - 1)
  }

  override def serviceName(): String = ???

  override def close(): Unit = ???

  override def listObjectsV2Paginator(listObjectsV2Request: ListObjectsV2Request): ListObjectsV2Iterable =
    new ListObjectsV2Iterable(this, listObjectsV2Request)

  override def listObjectsV2(listObjectsV2Request: ListObjectsV2Request): ListObjectsV2Response = {
    val prefix            = Option(listObjectsV2Request.prefix())
    val continuationToken = Option(listObjectsV2Request.continuationToken())
    val page              = pages.find(_.matchesPage(continuationToken)).getOrElse(fail("No page matched"))

    val startsWith = page.keys.filter(key => prefix.isEmpty || prefix.exists(key.startsWith))
    val commonPrefixes = startsWith.map {
      key =>
        val lastInd = key.lastIndexOf("/") + 1
        val pre     = key.substring(0, lastInd)
        CommonPrefix.builder().prefix(pre).build()
    }
    ListObjectsV2Response
      .builder()
      .prefix(prefix.orNull)
      .contents(startsWith.map(e => S3Object.builder().key(e).build()).asJava)
      .commonPrefixes(commonPrefixes.asJava)
      .continuationToken(page.continuationToken.orNull)
      .nextContinuationToken(page.nextContinuationToken.orNull)
      .build()
  }

}
