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
package io.lenses.streamreactor.connect.cloud.common.source.state

import cats.effect.IO
import io.lenses.streamreactor.connect.cloud.common.source.distribution.PartitionSearcherResponse

/**
  * Trait defining a partition searcher.
  * Implementations of this trait are responsible for finding partitions based on previous search results.
  */
trait PartitionSearcher {

  /**
    * Finds partitions based on the provided last found partition responses.
    *
    * @param lastFound The previously found partition responses.
    * @return          An IO monad containing a sequence of new partition responses.
    */
  def find(
    lastFound: Seq[PartitionSearcherResponse],
  ): IO[Seq[PartitionSearcherResponse]]
}
