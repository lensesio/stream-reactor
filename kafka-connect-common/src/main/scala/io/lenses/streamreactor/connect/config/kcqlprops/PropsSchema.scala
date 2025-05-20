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
package io.lenses.streamreactor.connect.config.kcqlprops

import enumeratum._

trait PropsSchema {}

case class EnumPropsSchema[T <: Enum[_]](t: T) extends PropsSchema

object StringPropsSchema extends PropsSchema

object IntPropsSchema extends PropsSchema

object LongPropsSchema extends PropsSchema

object BooleanPropsSchema extends PropsSchema

object CharPropsSchema extends PropsSchema

case class MapPropsSchema[K, V]() extends PropsSchema
