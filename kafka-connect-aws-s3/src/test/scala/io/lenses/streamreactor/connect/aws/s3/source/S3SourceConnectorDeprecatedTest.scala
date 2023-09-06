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
package io.lenses.streamreactor.connect.aws.s3.source

import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.TASK_INDEX
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.util
import scala.jdk.CollectionConverters.MapHasAsScala

class S3SourceConnectorDeprecatedTest extends AnyFlatSpecLike with Matchers with OptionValues {

  "taskConfigs" should "deliver correct number of task configs" in {

    val sourceConnector = new S3SourceConnectorDeprecated()
    val taskConfigs: util.List[util.Map[String, String]] = sourceConnector.taskConfigs(5)

    taskConfigs.get(0).asScala.get(TASK_INDEX).value shouldBe "0:5"
    taskConfigs.get(1).asScala.get(TASK_INDEX).value shouldBe "1:5"
    taskConfigs.get(2).asScala.get(TASK_INDEX).value shouldBe "2:5"
    taskConfigs.get(3).asScala.get(TASK_INDEX).value shouldBe "3:5"
    taskConfigs.get(4).asScala.get(TASK_INDEX).value shouldBe "4:5"

  }

}
