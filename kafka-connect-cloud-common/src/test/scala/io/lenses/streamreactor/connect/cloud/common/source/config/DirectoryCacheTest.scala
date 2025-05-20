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
package io.lenses.streamreactor.connect.cloud.common.source.config

import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class DirectoryCacheTest extends AnyFunSuiteLike with Matchers with MockitoSugar {

  test("ensureDirectoryExists should return Right(Unit) and add the directory to the cache if it does not exist") {
    val storageInterface = mock[StorageInterface[_]]
    when(storageInterface.createDirectoryIfNotExists("bucket", "path")).thenReturn(Right(()))
    val cache = new DirectoryCache(storageInterface)
    cache.ensureDirectoryExists("bucket", "path") should be(Right(()))
    cache.ensureDirectoryExists("bucket", "path") should be(Right(()))
    verify(storageInterface, times(1)).createDirectoryIfNotExists("bucket", "path")
  }

  test(
    "ensureDirectoryExists should return Left(FileCreateError) and not add to the cache if creating the directory fails",
  ) {
    val storageInterface = mock[StorageInterface[_]]
    val error            = FileCreateError(new IllegalStateException("Bad"), "data")
    when(storageInterface.createDirectoryIfNotExists("bucket", "path")).thenReturn(Left(error))
    val cache = new DirectoryCache(storageInterface)
    cache.ensureDirectoryExists("bucket", "path") should be(Left(error))
    cache.ensureDirectoryExists("bucket", "path") should be(Left(error))
    verify(storageInterface, times(2)).createDirectoryIfNotExists("bucket", "path")
  }

  test("ensureDirectoryExists should not add the directory to the cache if it already exists") {
    val storageInterface = mock[StorageInterface[_]]
    when(storageInterface.createDirectoryIfNotExists("bucket", "path")).thenReturn(Right(()))
    val cache = new DirectoryCache(storageInterface)
    cache.ensureDirectoryExists("bucket", "path") should be(Right(()))
    cache.ensureDirectoryExists("bucket", "path") should be(Right(()))
    verify(storageInterface, times(1)).createDirectoryIfNotExists("bucket", "path")
    cache.ensureDirectoryExists("bucket", "path") should be(Right(()))
    verify(storageInterface, times(1)).createDirectoryIfNotExists("bucket", "path")
  }
}
