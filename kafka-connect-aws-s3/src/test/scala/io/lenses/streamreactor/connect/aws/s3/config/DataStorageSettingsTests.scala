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

import io.lenses.streamreactor.connect.aws.s3.config.DataStorageSettings.AllFields
import org.apache.kafka.common.config.ConfigException
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataStorageSettingsTests extends AnyFunSuite with Matchers {

  test("empty properties return defaults") {
    val properties      = Map.empty[String, String]
    val storageSettings = DataStorageSettings.from(properties).getOrElse(fail("Should not have failed"))
    storageSettings shouldBe DataStorageSettings.Default
  }

  test("only envelope set to true returns all true") {
    val properties      = Map(DataStorageSettings.StoreEnvelopeKey -> "true")
    val storageSettings = DataStorageSettings.from(properties)
    storageSettings shouldBe Right(DataStorageSettings(
      envelope = true,
      key      = true,
      value    = true,
      metadata = true,
      headers  = true,
    ))
  }

  test("only envelope set to true but not all fields set returns error") {
    val properties = Map(DataStorageSettings.StoreEnvelopeKey -> "true", DataStorageSettings.StoreKeyKey -> "false")
    assertOnError(
      DataStorageSettings.from(properties),
      s"If ${DataStorageSettings.StoreEnvelopeKey} is set to true, then setting selective fields is not allowed. Either set them all or leave them out, they default to true.",
    )
  }

  test("envelope set to true but all fields false returns error") {
    val properties = Map(
      DataStorageSettings.StoreEnvelopeKey -> "true",
      DataStorageSettings.StoreKeyKey      -> "false",
      DataStorageSettings.StoreValueKey    -> "false",
      DataStorageSettings.StoreMetadataKey -> "false",
      DataStorageSettings.StoreHeadersKey  -> "false",
    )

    assertOnError(
      DataStorageSettings.from(properties),
      s"If ${DataStorageSettings.StoreEnvelopeKey} is set to true then at least one of ${AllFields.mkString("[", ",", "]")} must be set to true.",
    )
  }

  test("only keys enabled") {
    val properties = Map(
      DataStorageSettings.StoreEnvelopeKey -> "true",
      DataStorageSettings.StoreKeyKey      -> "true",
      DataStorageSettings.StoreValueKey    -> "false",
      DataStorageSettings.StoreMetadataKey -> "false",
      DataStorageSettings.StoreHeadersKey  -> "false",
    )
    val storageSettings = DataStorageSettings.from(properties)
    storageSettings shouldBe Right(DataStorageSettings(
      envelope = true,
      key      = true,
      value    = false,
      metadata = false,
      headers  = false,
    ))
  }

  test("metadata enabled") {
    val properties = Map(
      DataStorageSettings.StoreEnvelopeKey -> "true",
      DataStorageSettings.StoreKeyKey      -> "false",
      DataStorageSettings.StoreValueKey    -> "false",
      DataStorageSettings.StoreMetadataKey -> "true",
      DataStorageSettings.StoreHeadersKey  -> "false",
    )
    val storageSettings = DataStorageSettings.from(properties)
    storageSettings shouldBe Right(DataStorageSettings(
      envelope = true,
      key      = false,
      value    = false,
      metadata = true,
      headers  = false,
    ))
  }
  test("headers enabled") {
    val properties = Map(
      DataStorageSettings.StoreEnvelopeKey -> "true",
      DataStorageSettings.StoreKeyKey      -> "false",
      DataStorageSettings.StoreValueKey    -> "false",
      DataStorageSettings.StoreMetadataKey -> "false",
      DataStorageSettings.StoreHeadersKey  -> "true",
    )
    val storageSettings = DataStorageSettings.from(properties)
    storageSettings shouldBe Right(DataStorageSettings(
      envelope = true,
      key      = false,
      value    = false,
      metadata = false,
      headers  = true,
    ))
  }
  test("all enabled") {
    val properties = Map(
      DataStorageSettings.StoreEnvelopeKey -> "true",
      DataStorageSettings.StoreKeyKey      -> "true",
      DataStorageSettings.StoreValueKey    -> "true",
      DataStorageSettings.StoreMetadataKey -> "true",
      DataStorageSettings.StoreHeadersKey  -> "true",
    )
    val storageSettings = DataStorageSettings.from(properties)
    storageSettings shouldBe Right(DataStorageSettings(
      envelope = true,
      key      = true,
      value    = true,
      metadata = true,
      headers  = true,
    ))
  }
  test("invalid envelope value") {
    val properties = Map(DataStorageSettings.StoreEnvelopeKey -> "invalid")
    assertOnError(
      DataStorageSettings.from(properties),
      s"Invalid value for configuration [${DataStorageSettings.StoreEnvelopeKey}]. The value must be one of: true, false.",
    )
  }
  test("invalid key value") {
    val properties = Map(DataStorageSettings.StoreEnvelopeKey -> "true", DataStorageSettings.StoreKeyKey -> "invalid")
    assertOnError(
      DataStorageSettings.from(properties),
      s"Invalid value for configuration [${DataStorageSettings.StoreKeyKey}]. The value must be one of: true, false.",
    )
  }
  test("invalid metadata value") {
    val properties =
      Map(DataStorageSettings.StoreEnvelopeKey -> "true", DataStorageSettings.StoreMetadataKey -> "invalid")
    assertOnError(
      DataStorageSettings.from(properties),
      s"Invalid value for configuration [${DataStorageSettings.StoreMetadataKey}]. The value must be one of: true, false.",
    )
  }
  test("invalid headers value") {
    val properties =
      Map(DataStorageSettings.StoreEnvelopeKey -> "true", DataStorageSettings.StoreHeadersKey -> "invalid")
    assertOnError(
      DataStorageSettings.from(properties),
      s"Invalid value for configuration [${DataStorageSettings.StoreHeadersKey}]. The value must be one of: true, false.",
    )
  }
  test("invalid value value") {
    val properties = Map(DataStorageSettings.StoreEnvelopeKey -> "true", DataStorageSettings.StoreValueKey -> "invalid")

    assertOnError(
      DataStorageSettings.from(properties),
      s"Invalid value for configuration [${DataStorageSettings.StoreValueKey}]. The value must be one of: true, false.",
    )
  }

  private def assertOnError(actual: Either[ConfigException, DataStorageSettings], msg: String): Assertion =
    actual match {
      case Left(value) =>
        value.getMessage shouldBe msg
      case Right(_) => fail("Should  have failed.")
    }
}
