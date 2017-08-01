/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.connect.cassandra.cdc.logs

import java.nio.ByteBuffer
import java.util

import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.CdcConfig
import com.datamountaineer.streamreactor.connect.cassandra.cdc.metadata.ConnectSchemaBuilder
import org.apache.cassandra.db.marshal._
import org.apache.cassandra.db.rows._
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.collection.JavaConversions._

/**
  * Stores the change into the Connect Struct.
  */
object PutColumnChange {

  def buildFromUDT(struct: Struct, input: ByteBuffer, ut: UserType): Unit = {

    (0 until ut.size()).foreach { i =>

      val fieldName = ut.fieldNameAsString(i)
      val fieldType = ut.fieldType(i)
      val fieldBuffer = ByteBufferUtil.readBytes(input, input.getInt())

      val fieldValue = fieldType.getSerializer.deserialize(fieldBuffer) match {
        case hs: util.HashSet[_] => new util.ArrayList[Object](hs.asInstanceOf[util.HashSet[Object]])
        case other => other
      }
      struct.put(fieldName, fieldValue)
    }
  }

  def apply(schema: Schema, cd: ColumnData, cellPath: List[String], deletedColumns: util.ArrayList[String])(implicit config: CdcConfig): (Boolean, Any) = {
    if (cd.column().isSimple) {
      val cell = cd.asInstanceOf[Cell]
      val value = if (cell.isTombstone) {
        deletedColumns.add(cellPath.mkString("."))
        null
      } else {
        val valueType = cd.column().cellValueType()
        if (valueType.isUDT) {
          val ut = valueType.asInstanceOf[UserType]
          val udStruct = new Struct(schema)
          buildFromUDT(udStruct, cell.value(), ut)
          udStruct
        } else {
          val cellValue = cell.column.`type`.getSerializer.deserialize(cell.value())
          //we handle these extra because of the output type in the Connect Source Record Struct
          ConnectSchemaBuilder.coerceValue(
            cellValue,
            cell.column().`type`,
            schema
          )
        }
      }

      cell.isTombstone -> value
    } else {
      //we have a complex type
      cd match {
        case complexData: ComplexColumnData =>

          val fieldName = cd.column().name.toString

          var isTombstone = false

          val value = cd.column().`type` match {
            case l: ListType[_] =>
              val list = new util.ArrayList[Object]
              complexData.foreach { cell =>
                val (tombstone, v) = apply(schema.valueSchema(), cell, cellPath :+ fieldName, deletedColumns) //l.valueComparator().getSerializer.deserialize(cell.value())
                if (!tombstone) {
                  list.add(v.asInstanceOf[Object])
                } else {
                  isTombstone = true
                  deletedColumns.add(cellPath.mkString("."))
                }
              }
              if (list.nonEmpty) list else null
            case m: MapType[_, _] =>
              val map = new util.HashMap[Object, Object]()
              complexData.foreach { cell =>
                val (tombstone, (k, v)) = apply(schema.valueSchema(), cell, cellPath :+ fieldName, deletedColumns)
                if (!tombstone) {
                  map.put(k.asInstanceOf[Object], v.asInstanceOf[Object])
                }
                else {
                  isTombstone = true
                  deletedColumns.add(cellPath.mkString("."))
                }
              }
              if (map.nonEmpty) map else null
            case s: SetType[_] =>
              val list = new util.ArrayList[Object]
              complexData.foreach { cell =>
                val (tombstone, v) = apply(schema.valueSchema(), cell, cellPath :+ fieldName, deletedColumns)
                if (!tombstone) {
                  list.add(v.asInstanceOf[Object])
                } else {
                  isTombstone = true
                  deletedColumns.add(cellPath.mkString("."))
                }
              }
              if (list.nonEmpty) list else null
            case ut: UserType =>
              var udStruct: Struct = null

              complexData.foreach { cell =>
                val id: Short = ut.nameComparator.getSerializer.deserialize(cell.path.get(0))
                val fieldName = ut.fieldNameAsString(id)

                if (cell.isTombstone) {
                  isTombstone = true
                  deletedColumns.add((cellPath :+ fieldName).mkString("."))
                } else {
                  if (udStruct == null) {
                    udStruct = new Struct(schema)
                  }
                  udStruct.put(fieldName, ut.fieldType(id).getSerializer.deserialize(cell.value()))
                }
              }
              udStruct
            case other => throw new IllegalArgumentException(s"Error processing change for column:${cd.column().name.toString}. Column type of '$other' is not supported.")
          }

          isTombstone -> value

        case input: BufferCell =>
          val fieldName = cd.column().name.toString

          var isTombstone = input.isTombstone
          val value = if (isTombstone) {
            deletedColumns.add(cellPath.mkString("."))
            null
          } else {
            val cellValue = input.value()
            input.column().`type` match {
              case l: ListType[_] =>

                if (l.getElementsType.isUDT) {
                  val elementStruct = new Struct(schema)
                  buildFromUDT(elementStruct, cellValue, l.getElementsType.asInstanceOf[UserType])
                  elementStruct
                } else {
                  l.valueComparator().getSerializer.deserialize(cellValue)
                }

              case m: MapType[_, _] =>
                val entryKey = m.nameComparator().getSerializer.deserialize(input.path().get(0))

                val entryValue = if (m.getValuesType.isUDT) {
                  var valueStruct = new Struct(schema)
                  //val fieldBuffer = ByteBufferUtil.readBytes(cellValue, cellValue.getInt())

                  buildFromUDT(valueStruct, cellValue, m.getValuesType.asInstanceOf[UserType])
                  valueStruct
                } else {
                  m.valueComparator().getSerializer.deserialize(cellValue)
                }
                entryKey -> entryValue

              case s: SetType[_] =>
                if (s.getElementsType().isUDT) {
                  val elementStruct = new Struct(schema)
                  buildFromUDT(elementStruct, input.path().get(0), s.getElementsType().asInstanceOf[UserType])
                  elementStruct
                } else {
                  s.getElementsType.getSerializer.deserialize(input.path().get(0))
                }

              case ut: UserType =>
                val udStruct = new Struct(schema)
                buildFromUDT(udStruct, cellValue, ut)
                udStruct
              case other => throw new IllegalArgumentException(s"Error processing change for column:${cd.column().name.toString}. Column type of '$other' is not supported.")
            }
          }
          isTombstone -> value

      }
    }
  }
}
