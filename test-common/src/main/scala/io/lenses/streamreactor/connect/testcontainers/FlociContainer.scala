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
package io.lenses.streamreactor.connect.testcontainers

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.net.URI

/**
 * Floci (`floci/floci`), an AWS emulator whose S3 honours `If-Match` on `DeleteObject` and the
 * per-object `ETag` of `DeleteObjects`, answering mismatches and missing keys the way AWS S3 does.
 * Used for the S3 conditional-delete IT; s3proxy (`S3Container`) ignores the condition. Plain
 * `GenericContainer`: no Floci Testcontainers module, so no Testcontainers version bump. Accepts
 * any static credentials; use path-style addressing.
 */
class FlociContainer(dockerTag: String = FlociContainer.DefaultTag)
    extends GenericContainer[FlociContainer](DockerImageName.parse("floci/floci").withTag(dockerTag)) {

  withExposedPorts(FlociContainer.Port)
  waitingFor(Wait.forListeningPort())

  def getEndpointUrl: URI = new URI(s"http://$getHost:${getMappedPort(FlociContainer.Port)}")
}

object FlociContainer {
  // Pinned: the emulator's conditional-delete behaviour was verified on this tag.
  val DefaultTag: String = "2.5.0"
  val Port:       Int    = 4566
  val AccessKey:  String = "test"
  val SecretKey:  String = "test"
  val Region:     String = "us-east-1"
}
