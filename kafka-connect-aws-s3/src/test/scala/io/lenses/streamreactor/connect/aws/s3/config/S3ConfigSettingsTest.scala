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
package io.lenses.streamreactor.connect.aws.s3.config

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfigDef
import io.lenses.streamreactor.connect.aws.s3.source.config.S3SourceConfigDef
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * Reads the constants in the S3ConfigSettings class and ensures that they are
  * all lower cased for consistency.  This will help protect for regressions in
  * future when adding new properties to this file.  Uses reflection.
  */
class S3ConfigSettingsTest extends AnyFlatSpec with Matchers with LazyLogging {

  "S3ConfigSettings" should "ensure all keys are lower case" in {

    val configKeys =
      S3SinkConfigDef.config.configKeys().keySet().asScala ++ S3SourceConfigDef.config.configKeys().keySet().asScala

    configKeys.size shouldBe 48
    configKeys.foreach {
      k => k.toLowerCase should be(k)
    }
  }
}
