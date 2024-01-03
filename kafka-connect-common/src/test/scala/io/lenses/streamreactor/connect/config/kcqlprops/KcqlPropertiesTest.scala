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
package io.lenses.streamreactor.connect.config.kcqlprops

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import enumeratum.Enum
import enumeratum.EnumEntry
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties.stringToInt
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties.stringToString
import org.scalatest.OptionValues

class KcqlPropertiesTest extends AnyFunSuite with Matchers with OptionValues {

  private sealed trait MyEnum extends EnumEntry

  private object MyEnum extends Enum[MyEnum] {
    case object Key1_Happy extends MyEnum

    case object Key2_Mismatch extends MyEnum

    case object Key3_Missing extends MyEnum
    case object Key4_Missing extends MyEnum

    case object Key5_Multi     extends MyEnum
    case object Key6_Empty_Map extends MyEnum
    case object Key7_Map       extends MyEnum

    val values: IndexedSeq[MyEnum] = findValues
  }

  private val sampleMap: Map[String, String] = Map(
    MyEnum.Key1_Happy.entryName              -> "0",
    MyEnum.Key2_Mismatch.entryName           -> "1",
    MyEnum.Key3_Missing.entryName            -> "",
    MyEnum.Key5_Multi.entryName              -> "AB",
    MyEnum.Key7_Map.entryName + ".offset"    -> "1",
    MyEnum.Key7_Map.entryName + ".partition" -> "2",
  )

  private val kcqlPropsSchema: KcqlPropsSchema[MyEnum, MyEnum.type] = KcqlPropsSchema(
    MyEnum,
    Map[MyEnum, PropsSchema](
      MyEnum.Key1_Happy     -> CharPropsSchema,
      MyEnum.Key2_Mismatch  -> IntPropsSchema,
      MyEnum.Key3_Missing   -> CharPropsSchema,
      MyEnum.Key4_Missing   -> CharPropsSchema,
      MyEnum.Key5_Multi     -> CharPropsSchema,
      MyEnum.Key6_Empty_Map -> MapPropsSchema[String, Int](),
      MyEnum.Key7_Map       -> MapPropsSchema[String, Int](),
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

  test("getOptionalMap should return empty when no properties specified") {
    kcqlProps.getOptionalMap(MyEnum.Key6_Empty_Map, stringToString, stringToInt) shouldEqual None
  }

  test("getOptionalMap should return values when multiple properties specified") {
    kcqlProps.getOptionalMap(MyEnum.Key7_Map, stringToString, stringToInt).value shouldEqual Map[String, Int](
      "offset"    -> 1,
      "partition" -> 2,
    )
  }
}
