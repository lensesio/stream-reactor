/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.storage

import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

/**
 * LC-318: shared contract asserting that every `StorageInterface` backend excludes directory
 * markers (keys ending in "/", e.g. console-created "folders") and zero-byte placeholder
 * objects from `list`, `listKeysRecursive` and `listFileMetaRecursive`, mirroring
 * `AwsS3StorageInterface`'s original filtering.
 *
 * `kafka-connect-aws-s3` and `kafka-connect-gcp-storage` both depend on `cloud-common` via
 * `test->test` in build.sbt, which makes this trait visible to both modules' test sources.
 * Each backend's test suite mixes this trait in and implements the fixture hooks below using
 * whichever client-mocking approach suits that backend; a divergence in either backend's
 * filtering will fail here.
 *
 * Deliberately NOT covered here: pagination/resume-marker semantics for a page that is
 * *entirely* filtered out. Both S3 (LC-344) and GCS keep the raw last object as their marker so
 * pagination still advances; that is exercised by backend-specific regression tests, not this
 * shared contract.
 *
 * Also not covered: unknown/null object size. GCS fails open (keeps the object when
 * Blob.getSize is null) so a listing that omits size cannot silently starve the source; S3's
 * `_.size() > 0` throws on a null Long and surfaces a FileListError. Aligning S3 would be a
 * behaviour change and is out of scope for LC-318.
 */
trait StorageInterfaceListFilteringBehaviour extends AnyFlatSpecLike with Matchers with EitherValues with OptionValues {

  /** One object as it would appear in a raw (unfiltered) listing. */
  case class FilterFixtureObject(key: String, sizeBytes: Long)

  protected val validKey:    String = "topic/0/0001.json"
  protected val zeroByteKey: String = "topic/0/zero-byte-placeholder.json"
  protected val markerKey:   String = "topic/0/"

  /**
   * A valid data object alongside a zero-byte placeholder and a "/"-suffixed directory marker.
   * Each filter predicate has exactly one witness: the zero-byte key pins only the size rule,
   * and the marker (non-zero size) pins only the "/"-suffix rule. Giving the marker size 0
   * would let either predicate alone pass the suite, leaving half of LC-318 untested.
   */
  protected val mixedFixture: Seq[FilterFixtureObject] = Seq(
    FilterFixtureObject(validKey, 42L),
    FilterFixtureObject(zeroByteKey, 0L), // pins the zero-byte rule only
    FilterFixtureObject(markerKey, 42L),  // pins the "/"-suffix rule only
  )

  /**
   * Exercises `StorageInterface.list` against a backend whose client is mocked to return
   * exactly `objects` for a single page/response.
   */
  def listWithFixture(objects: Seq[FilterFixtureObject]): Either[FileListError, Option[ListOfKeysResponse[_]]]

  /**
   * Exercises `StorageInterface.listKeysRecursive` against a backend whose client is mocked to
   * return exactly `objects` across its recursive listing (a single page/iteration suffices).
   */
  def listKeysRecursiveWithFixture(
    objects: Seq[FilterFixtureObject],
  ): Either[FileListError, Option[ListOfKeysResponse[_]]]

  /**
   * Exercises `StorageInterface.listFileMetaRecursive` against a backend whose client is
   * mocked to return exactly `objects` across its recursive listing.
   */
  def listFileMetaRecursiveWithFixture(
    objects: Seq[FilterFixtureObject],
  ): Either[FileListError, Option[ListOfMetadataResponse[_ <: FileMetadata]]]

  "list" should "exclude zero-byte objects and directory markers from the returned keys" in {
    listWithFixture(mixedFixture).value.value.files should contain only validKey
  }

  "listKeysRecursive" should "exclude zero-byte objects and directory markers from the returned keys" in {
    listKeysRecursiveWithFixture(mixedFixture).value.value.files should contain only validKey
  }

  "listFileMetaRecursive" should "exclude zero-byte objects and directory markers from the returned keys" in {
    listFileMetaRecursiveWithFixture(mixedFixture).value.value.files.map(_.file) should contain only validKey
  }
}
