/*
 * Copyright 2017-2026 Lenses.io Ltd
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

import com.datastax.oss.common.sink.record.StructDataMetadata
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory
import io.lenses.streamreactor.connect.cassandra.adapters.StructAdapter

import scala.jdk.CollectionConverters._

class StructToUDTCodec(codecFactory: ConvertingCodecFactory, definedType: UserDefinedType)
    extends ConvertingCodec[StructAdapter, UdtValue](
      codecFactory.getCodecRegistry.codecFor(definedType).asInstanceOf[TypeCodec[UdtValue]],
      classOf[StructAdapter],
    ) {

  private val fieldNames = definedType.getFieldNames.asScala.toList
  private val fieldTypes = definedType.getFieldTypes.asScala.toList
  private val fieldTuple = fieldNames.zip(fieldTypes)

  override def externalToInternal(external: StructAdapter): UdtValue = Option(external) match {
    case None         => null
    case Some(struct) => convertStructToUdt(struct)
  }

  private def convertStructToUdt(external: StructAdapter): UdtValue = {
    val schema         = external.schema()
    val structMetadata = new StructDataMetadata(schema)
    val structFieldNames: Set[String] = schema.fields.asScala.foldLeft(Set.empty[String]) { (set, field) =>
      set + field.name
    }

    validateFieldCount(structFieldNames.size)
    val initialValue = definedType.newValue

    fieldTuple.foldLeft(initialValue) { case (value, (fieldName, fieldType)) =>
      if (!structFieldNames.contains(fieldName.asInternal())) {
        throw new IllegalArgumentException(
          s"Field '$fieldName' not found in the provided struct for UDT ${definedType.getName.asInternal}",
        )
      }
      val fieldCodec = createFieldCodec(fieldType, structMetadata, fieldName.asInternal)
      val fieldValue = fieldCodec.externalToInternal(external.get(fieldName.asInternal))
      value.set(fieldName, fieldValue, fieldCodec.getInternalJavaType)
    }
  }

  private def validateFieldCount(structFieldCount: Int): Unit = {
    val expectedCount = definedType.getFieldNames.size
    if (structFieldCount != expectedCount) {
      throw new IllegalArgumentException(
        s"Expecting $expectedCount fields but got $structFieldCount in UDT ${definedType.getName.asInternal}",
      )
    }
  }

  private def createFieldCodec(
    fieldType:      DataType,
    structMetadata: StructDataMetadata,
    fieldName:      String,
  ): ConvertingCodec[Any, Any] = {
    val fieldTypeGeneric = structMetadata.getFieldType(fieldName, fieldType).asInstanceOf[GenericType[Any]]
    codecFactory.createConvertingCodec(fieldType, fieldTypeGeneric, false)
  }

  override def internalToExternal(internal: UdtValue): StructAdapter = Option(internal) match {
    case None => null
    case Some(_) => throw new UnsupportedOperationException(
        "Converting from UDT to Struct is not supported in this codec.",
      )
  }
}
