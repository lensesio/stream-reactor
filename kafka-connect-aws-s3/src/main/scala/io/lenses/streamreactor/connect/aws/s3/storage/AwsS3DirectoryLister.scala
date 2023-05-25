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

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable

import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala

abstract class AwsS3DirectoryLister(
  implicit
  connectorTask: ConnectorTaskId,
  s3Client:      S3Client,
) extends StorageInterface {
  def findDirectories(
    bucketAndPrefix:  S3Location,
    completionConfig: DirectoryFindCompletionConfig,
    exclude:          Set[String],
    continueFrom:     Option[String],
  ): IO[DirectoryFindResults] =
    for {
      listObjsReq <- createListObjectsRequest(bucketAndPrefix, continueFrom)
      paginator   <- IO(s3Client.listObjectsV2Paginator(listObjsReq))
      prefixInfo  <- extractPrefixesFromResponse(paginator, exclude, completionConfig)
      flattened   <- flattenPrefixes(bucketAndPrefix, prefixInfo.partitions, completionConfig, exclude)
    } yield {
      prefixInfo match {
        case CompletedDirectoryFindResults(_)        => CompletedDirectoryFindResults(flattened)
        case a @ PausedDirectoryFindResults(_, _, _) => a.copy(flattened)
      }
    }

  private def flattenPrefixes(
    bucketAndPrefix:  S3Location,
    prefixes:         Set[String],
    completionConfig: DirectoryFindCompletionConfig,
    exclude:          Set[String],
  ): IO[Set[String]] =
    if (completionConfig.levelsToRecurse == 0) IO.delay(prefixes)
    else
      for {
        ioPrefixes <- IO.delay(prefixes)
        bap        <- IO.delay(ioPrefixes.map(bucketAndPrefix.fromRoot))
        recursiveResult <- bap.toList
          .traverse(
            findDirectories(
              _,
              completionConfig.copy(levelsToRecurse = completionConfig.levelsToRecurse - 1),
              exclude,
              Option.empty,
            ).map(_.partitions),
          ).map {
            listSetString: List[Set[String]] =>
              listSetString.toSet.flatten
          }
      } yield {
        recursiveResult.headOption.fold(ioPrefixes)(_ => recursiveResult)
      }

  private def createListObjectsRequest(
    bucketAndPrefix: S3Location,
    lastFound:       Option[String],
  ): IO[ListObjectsV2Request] =
    IO.delay {
      val builder = ListObjectsV2Request
        .builder()
        .maxKeys(1000)
        .bucket(bucketAndPrefix.bucket)
        .delimiter("/")
        .startAfter(lastFound.orNull)
      bucketAndPrefix.prefix.map(addTrailingSlash).foreach(builder.prefix)
      builder.build()
    }

  private def extractPrefixesFromResponse(
    listObjectsV2Response: ListObjectsV2Iterable,
    exclude:               Set[String],
    completionConfig:      DirectoryFindCompletionConfig,
  ): IO[DirectoryFindResults] =
    IO {
      val rtn: mutable.Set[String] = mutable.Set()
      val resp = listObjectsV2Response
        .iterator()
        .asScala

      var partialResultInfo: Option[(String, String)] = none

      while (resp.hasNext && partialResultInfo.isEmpty) {
        val listResp: ListObjectsV2Response = resp.next()
        val commonPrefixesFiltered = listResp
          .commonPrefixes()
          .asScala
          .map(_.prefix())
          .filter(connectorTask.ownsDir)
          .filterNot(exclude.contains)
        rtn.addAll(commonPrefixesFiltered)

        partialResultInfo = {
          for {
            stopReason <- completionConfig.stopReason(rtn.size)
          } yield {
            stopReason.map(sr => (sr, listResp.contents().get(listResp.contents().size() - 1).key()))
          }
        }.unsafeRunSync()

      }

      partialResultInfo.map {
        case (sr, lf) => PausedDirectoryFindResults(rtn.toSet, sr, lf)
      }.getOrElse(CompletedDirectoryFindResults(rtn.toSet))
    }

  private def addTrailingSlash(in: String): String =
    if (!in.endsWith("/")) in + "/" else in

}
