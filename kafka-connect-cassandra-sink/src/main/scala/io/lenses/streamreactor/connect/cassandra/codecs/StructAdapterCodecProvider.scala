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
package io.lenses.streamreactor.connect.cassandra.codecs

import com.datastax.oss.common.sink.AbstractStruct
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecProvider
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.cassandra.adapters.StructAdapter

import java.util.Optional

class StructAdapterCodecProvider extends ConvertingCodecProvider with StrictLogging {
  override def maybeProvide(
    cqlType:          DataType,
    externalJavaType: GenericType[_],
    codecFactory:     ConvertingCodecFactory,
    rootCodec:        Boolean,
  ): Optional[ConvertingCodec[_, _]] =
    (cqlType, externalJavaType) match {
      case (udt: UserDefinedType, extType) if StructAdapterCodecProvider.isStructType(extType) =>
        Optional.of(new StructToUDTCodec(codecFactory, udt))
      case _ =>
        logger.debug(
          s"StructAdapterCodecProvider: No codec provided for cqlType: $cqlType and externalJavaType: $externalJavaType",
        )
        Optional.empty()
    }
}

object StructAdapterCodecProvider {
  private val structAdapterType  = GenericType.of(classOf[StructAdapter])
  private val abstractStructType = GenericType.of(classOf[AbstractStruct])
  private def isStructType(externalJavaType: GenericType[_]): Boolean =
    externalJavaType.equals(structAdapterType) || externalJavaType.equals(abstractStructType)
}
