/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.formats

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.connect.data.Struct

object StructValueLookup extends LazyLogging {

  private val supportedPrimitiveTypes = Set(
    classOf[java.lang.Boolean],
    classOf[java.lang.Byte],
    classOf[java.lang.Double],
    classOf[java.lang.Float],
    classOf[java.lang.Integer],
    classOf[java.lang.Long],
    classOf[java.lang.Short],
    classOf[java.lang.String],
  )

  def lookupFieldValueFromStruct(struct: Struct)(fieldName: String): Option[String] = {
    Option(struct.get(fieldName)) match {
      case Some(primitiveValue) if supportedPrimitiveTypes.exists(c => c.isInstance(primitiveValue)) =>
        Some(primitiveValue.toString)
      case Some(byteArray@Array(_*)) if byteArray.forall(entry => entry.isInstanceOf[Byte]) =>
        Some(new String(byteArray.asInstanceOf[Array[Byte]]))
      case Some(other) =>
        logger.error("Non-primitive values not supported: " + other.getClass);
        throw new IllegalArgumentException("Non-primitive values not supported")
      case None =>
        None
    }
  }

}
