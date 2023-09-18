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
package io.lenses.streamreactor.connect.aws.s3.sink.config.padding

import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.aws.s3.sink.RightPadPaddingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfigDefBuilder
import io.lenses.streamreactor.connect.aws.s3.sink.config.kcqlprops.S3SinkPropsSchema
import org.mockito.MockitoSugar._
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PaddingServiceTest extends AnyFunSuite with Matchers with EitherValues {
  private val paddingStrategy = PaddingType.LeftPad.toPaddingStrategy(12, '0')
  private val fields          = Map("offset" -> paddingStrategy)
  private val emptyProps      = S3SinkPropsSchema.schema.readProps()

  test("PaddingService should return a padder for the specified field") {
    val paddingService = new PaddingService(fields)
    paddingService.padderFor("offset").padString("123") shouldEqual "000000000123"
  }

  test("PaddingService should return an identity function when field not in fields") {
    val paddingService = new PaddingService(fields)
    paddingService.padderFor("other").padString("123") shouldEqual "123"
  }
  test("PaddingService should apply different padding strategies to multiple fields") {
    val fields = Map(
      "partition" -> RightPadPaddingStrategy(10, '0'),
      "offset"    -> paddingStrategy,
    )
    val paddingService = new PaddingService(fields)
    paddingService.padderFor("partition").padString("123") shouldEqual "1230000000"
    paddingService.padderFor("offset").padString("123") shouldEqual "000000000123"
  }

  test("PaddingService should return an error when both KCQL properties and ConfigDef padding configurations are set") {
    val rightPadConfigDef: S3SinkConfigDefBuilder = mockConfigDefPadding(RightPadPaddingStrategy(10, '-').some)
    val kcqlProps = S3SinkPropsSchema.schema.readProps(
      "padding.length.partition" -> "5",
      "padding.char"             -> "#",
      "padding.type"             -> "RightPad",
    )
    val paddingService = PaddingService.fromConfig(rightPadConfigDef, kcqlProps)

    paddingService.left.value.getMessage should startWith("Unable to process both padding")
  }

  test("PaddingService should respect padding defined in KCQL properties") {
    val kcqlProps = S3SinkPropsSchema.schema.readProps(
      "padding.length.partition" -> "5",
      "padding.char"             -> "#",
      "padding.type"             -> "RightPad",
    )
    val paddingService =
      PaddingService.fromConfig(mockConfigDefPadding(none), kcqlProps).getOrElse(fail("No padding service found"))

    paddingService.padderFor("partition").padString("123") shouldEqual "123##"

  }

  test("fromConfig should create a PaddingService from S3SinkConfigDefBuilder operating only on 'offset'") {
    val rightPadConfigDef: S3SinkConfigDefBuilder = mockConfigDefPadding(RightPadPaddingStrategy(10, '-').some)
    val paddingService =
      PaddingService.fromConfig(rightPadConfigDef, emptyProps).getOrElse(fail("No padding service found"))

    paddingService.padderFor("offset").padString("123") shouldEqual "123-------"
    paddingService.padderFor("other").padString("123") shouldEqual "123"
  }

  test("fromConfig should return a PaddingService with default values when no config is provided") {
    val emptyConfigDef: S3SinkConfigDefBuilder = mockConfigDefPadding(none)
    val paddingService =
      PaddingService.fromConfig(emptyConfigDef, emptyProps).getOrElse(fail("No padding service found"))

    paddingService.padderFor("offset").padString("123") shouldEqual "000000000123"
    paddingService.padderFor("other").padString("123") shouldEqual "123"
  }

  private def mockConfigDefPadding(maybePaddingStrategy: Option[RightPadPaddingStrategy]) = {
    val configDef = mock[S3SinkConfigDefBuilder]
    when(configDef.getPaddingStrategy).thenReturn(maybePaddingStrategy)
    configDef
  }
}
