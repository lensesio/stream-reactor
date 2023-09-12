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
package io.lenses.streamreactor.connect.config.kcqlprops

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import enumeratum.Enum
import enumeratum.EnumEntry

class KcqlPropertiesTest extends AnyFunSuite with Matchers {

  private sealed trait MyEnum extends EnumEntry

  private object MyEnum extends Enum[MyEnum] {
    case object Key1_Happy extends MyEnum

    case object Key2_Mismatch extends MyEnum

    case object Key3_Missing extends MyEnum
    case object Key4_Missing extends MyEnum

    case object Key5_Multi extends MyEnum

    val values: IndexedSeq[MyEnum] = findValues
  }

  private val sampleMap: Map[MyEnum, String] = Map(
    MyEnum.Key1_Happy    -> "0",
    MyEnum.Key2_Mismatch -> "1",
    MyEnum.Key3_Missing  -> "",
    MyEnum.Key5_Multi    -> "AB",
  )

  private val kcqlPropsSchema: KcqlPropsSchema[MyEnum, MyEnum.type] = KcqlPropsSchema(
    MyEnum,
    Map[MyEnum, PropsSchema](
      MyEnum.Key1_Happy    -> CharPropsSchema,
      MyEnum.Key2_Mismatch -> IntPropsSchema,
      MyEnum.Key3_Missing  -> CharPropsSchema,
      MyEnum.Key4_Missing  -> CharPropsSchema,
      MyEnum.Key5_Multi    -> CharPropsSchema,
    ),
  )
  private val kcqlProps = KcqlProperties(kcqlPropsSchema, sampleMap)

  test("getOptionalChar should return Some(Char) when schema matches") {
    kcqlProps.getOptionalChar(MyEnum.Key1_Happy) shouldEqual Some('0')
  }

  test("getOptionalChar should return None when schema doesn't match") {
    kcqlProps.getOptionalChar(MyEnum.Key2_Mismatch) shouldEqual None
  }

  test("getOptionalChar should return None when key is not found in the map") {
    kcqlProps.getOptionalChar(MyEnum.Key3_Missing) shouldEqual None
    kcqlProps.getOptionalChar(MyEnum.Key4_Missing) shouldEqual None
  }

  test("getOptionalChar should return empty when >1 chars in String") {
    kcqlProps.getOptionalChar(MyEnum.Key5_Multi) shouldEqual None
  }
}
