/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.model

case class PartitionNamePath(path: String*) {
  override def toString: String = path.mkString(".")

  val reservedCharacters = Set("/", "\\")

  def validateProtectedCharacters: Unit =
    path.foreach(validateProtectedCharacters)

  private def validateProtectedCharacters(name: String): Unit =
    require(name != null && name.trim.nonEmpty && reservedCharacters.forall(!name.contains(_)), "Name must not be empty and must not contain a slash character")

  def headOption: Option[String] = path.headOption
  def head: String = path.head
  def tail = PartitionNamePath(path.tail:_*)
  def hasTail = path.size > 1
  def size: Int = path.size
}
