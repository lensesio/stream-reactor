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
package io.lenses.streamreactor.connect.cloud.common.sink.optimization

import AttachLatestSchemaOptimizer.ConfluentAvroUnionSchemaName
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * Optimizes writing data (e.g., Avro, Parquet) by ensuring a consistent schema is used,
 * specifically targeting scenarios with backward-compatible schema evolution.
 *
 * Problem: Writing records sequentially with varying (but backward-compatible) schemas
 * (e.g., schema1, schema1, schema2, schema1, schema2) can trigger frequent flushes
 * in Avro/Parquet writers. Each schema change might force a new block/segment,
 * reducing file efficiency and write throughput.
 *
 * Solution: This class attaches the *latest* known schema to all outgoing records within
 * a write operation. It adapts records originally conforming to older, compatible schemas
 * to match the latest one before they reach the underlying writer.
 *
 * Benefit: Prevents flushes caused by compatible schema variations, improving write performance.
 * Avoids serialization exceptions that would occur if attempting to write an old-schema
 * record with a writer expecting the latest schema.
 */
class AttachLatestSchemaOptimizer extends StrictLogging {
  // Store latest schema (key and value) found per topic.
  private case class TopicSchema(key: Option[Schema], value: Option[Schema])
  private val latestSchemas = mutable.Map[String, TopicSchema]()

  def update(records: List[SinkRecord]): List[SinkRecord] =
    if (records.isEmpty) {
      records
    } else {
      updateCurrentSchemas(records)
      applyLatestSchema(records)
    }

  private def updateCurrentSchemas(records: List[SinkRecord]): Unit =
    records.foreach { record =>
      val topic                = record.topic()
      val recordKeySchemaOpt   = Option(record.keySchema())
      val recordValueSchemaOpt = Option(record.valueSchema())

      if (recordKeySchemaOpt.isDefined || recordValueSchemaOpt.isDefined) {
        val currentLatest      = latestSchemas.getOrElse(topic, TopicSchema(None, None))
        var updatedKeySchema   = currentLatest.key
        var updatedValueSchema = currentLatest.value
        var changed            = false

        (recordKeySchemaOpt, currentLatest.key) match {
          case (Some(newSchema), Some(currentSchema)) if isNewerSchema(newSchema, currentSchema) =>
            updatedKeySchema = Some(newSchema); changed = true
          case (Some(newSchema), None) =>
            updatedKeySchema = Some(newSchema); changed = true
          case _ =>
        }
        (recordValueSchemaOpt, currentLatest.value) match {
          case (Some(newSchema), Some(currentSchema)) if isNewerSchema(newSchema, currentSchema) =>
            updatedValueSchema = Some(newSchema); changed = true
          case (Some(newSchema), None) =>
            updatedValueSchema = Some(newSchema); changed = true
          case _ =>
        }
        if (changed) {
          latestSchemas(topic) = TopicSchema(updatedKeySchema, updatedValueSchema)
        }
      }
    }

  private def applyLatestSchema(records: List[SinkRecord]): List[SinkRecord] =
    records.map { record =>
      val topic = record.topic()
      latestSchemas.get(topic).fold(record) { latestTopicSchema =>
        val originalKeySchemaOpt   = Option(record.keySchema())
        val originalValueSchemaOpt = Option(record.valueSchema())

        val keyNeedsUpdate = latestTopicSchema.key.exists(latestKeySch =>
          originalKeySchemaOpt.exists(origKeySch => isNewerSchema(latestKeySch, origKeySch)),
        )

        // Determine if value schema needs update
        val valueNeedsUpdate = latestTopicSchema.value.exists(latestValueSch =>
          originalValueSchemaOpt.exists(origValueSch => isNewerSchema(latestValueSch, origValueSch)),
        )

        // If an update is needed for either key or value schema, proceed with potential adaptation
        if (keyNeedsUpdate || valueNeedsUpdate) {
          val targetKeySchema = if (keyNeedsUpdate) latestTopicSchema.key.orNull else originalKeySchemaOpt.orNull
          val targetValueSchema =
            if (valueNeedsUpdate) latestTopicSchema.value.orNull else originalValueSchemaOpt.orNull

          Try {
            // Adapt Key
            val finalKey =
              if (keyNeedsUpdate && originalKeySchemaOpt.isDefined && targetKeySchema != null) {
                adaptValue(record.key(), originalKeySchemaOpt.get, targetKeySchema)
              } else {
                record.key()
              }

            // Adapt Value
            val finalValue =
              if (valueNeedsUpdate && originalValueSchemaOpt.isDefined && targetValueSchema != null) {
                adaptValue(record.value(), originalValueSchemaOpt.get, targetValueSchema)
              } else {
                record.value()
              }

            // Create the new record
            new SinkRecord(
              record.topic(),
              record.kafkaPartition(),
              targetKeySchema,   // Use the determined target schema
              finalKey,          // Use the potentially adapted key
              targetValueSchema, // Use the determined target schema
              finalValue,        // Use the potentially adapted value
              record.kafkaOffset(),
              record.timestamp(),
              record.timestampType(),
              record.headers(),
            )
          } match {
            case Success(newRecord) => newRecord // Return the adapted record
            case Failure(exception) =>
              logger.error(
                s"Failed to adapt record data for topic ${record.topic()} offset ${record.kafkaOffset()}: ${exception.getMessage}",
                exception,
              )
              // Decide error handling: skip record, fail task, or return original? Failing fast is often safer.
              throw new DataException(
                s"Failed to adapt record data for topic ${record.topic()} offset ${record.kafkaOffset()}",
                exception,
              )
          }
        } else {
          record // No schema update needed, return original record
        }
      }
    }

  /**
   * Recursively adapts a value from its original schema to a target schema.
   * Handles Structs, Arrays, Maps, and primitive types.
   *
   * @param originalValue   The original data value.
   * @param originalSchema  The schema corresponding to the original value.
   * @param targetSchema    The target schema to adapt the value to.
   * @return The adapted value conforming to the target schema.
   * @throws DataException if adaptation is not possible (e.g., incompatible types, required fields missing).
   */
  @throws[DataException]
  private def adaptValue(originalValue: Any, originalSchema: Schema, targetSchema: Schema): Any =
    if (originalSchema == targetSchema) originalValue
    else {
      Option(originalValue).fold {
        if (targetSchema.isOptional || targetSchema.defaultValue() != null)
          targetSchema.defaultValue().asInstanceOf[Any]
        else
          throw new DataException(s"Cannot adapt null value to required schema '${targetSchema.name()}'")
      } { value =>
        // Check for union type conversions first
        val targetIsUnion   = isUnionSchema(targetSchema)
        val originalIsUnion = isUnionSchema(originalSchema)

        (targetIsUnion, originalIsUnion) match {
          case (true, false) =>
            // Simple type -> Union promotion (e.g., enum -> [enum, string])
            promoteToUnion(value, originalSchema, targetSchema)

          case (false, true) =>
            // Union -> Simple type extraction (e.g., [enum, string] -> enum)
            value match {
              case s: Struct => extractFromUnion(s, originalSchema, targetSchema)
              case other =>
                throw new DataException(
                  s"Expected Struct for union schema '${originalSchema.name()}', found ${other.getClass}",
                )
            }

          case _ =>
            // Standard type handling (no union conversion)
            targetSchema.`type`() match {
              case Schema.Type.STRUCT =>
                value match {
                  case s: Struct => adaptStruct(s, originalSchema, targetSchema)
                  case other =>
                    throw new DataException(
                      s"Expected Struct for schema '${targetSchema.name()}', found ${other.getClass}",
                    )
                }
              case Schema.Type.ARRAY =>
                if (originalSchema.`type`() != Schema.Type.ARRAY)
                  throw new DataException(
                    s"Schema mismatch: Expected ARRAY but original schema is ${originalSchema.`type`()}",
                  )
                value match {
                  case l: java.util.List[_] => adaptArray(l, originalSchema, targetSchema)
                  case other =>
                    throw new DataException(
                      s"Expected List for schema '${targetSchema.name()}', found ${other.getClass}",
                    )
                }
              case Schema.Type.MAP =>
                if (originalSchema.`type`() != Schema.Type.MAP)
                  throw new DataException(
                    s"Schema mismatch: Expected MAP but original schema is ${originalSchema.`type`()}",
                  )
                value match {
                  case m: java.util.Map[_, _] => adaptMap(m, originalSchema, targetSchema)
                  case other =>
                    throw new DataException(
                      s"Expected Map for schema '${targetSchema.name()}', found ${other.getClass}",
                    )
                }
              case _ => value
            }
        }
      }
    }

  /** Adapts a Struct recursively. */
  private def adaptStruct(originalStruct: Struct, originalSchema: Schema, targetSchema: Schema): Struct = {
    if (originalSchema.`type`() != Schema.Type.STRUCT)
      throw new DataException(s"Schema mismatch: Expected STRUCT but original schema is ${originalSchema.`type`()}")

    val newStruct = new Struct(targetSchema)
    targetSchema.fields().asScala.foreach { targetField =>
      val fieldName         = targetField.name()
      val targetFieldSchema = targetField.schema()

      // Find corresponding field in original schema
      val originalFieldOpt = Option(originalSchema.field(fieldName))

      originalFieldOpt match {
        case Some(originalField) =>
          // Field exists in both schemas: Get original value and adapt it recursively
          Try(originalStruct.get(originalField)) match {
            case Success(originalFieldValue) =>
              val adaptedFieldValue =
                adaptValue(originalFieldValue, originalField.schema(), targetFieldSchema) // Recursive call
              newStruct.put(targetField, adaptedFieldValue)
            case Failure(e) => // Should be rare if field exists
              throw new DataException(s"Failed to retrieve value for existing field '$fieldName' from original struct.",
                                      e,
              )
          }
        case None =>
          // Field only exists in target schema (new field): Use default or null if allowed
          if (targetFieldSchema.defaultValue() != null) {
            newStruct.put(targetField, targetFieldSchema.defaultValue())
          } else if (targetFieldSchema.isOptional) {
            newStruct.put(targetField, null)
          } else {
            throw new DataException(
              s"Cannot adapt struct: Required field '$fieldName' in target schema ('${targetSchema.name()}') is missing in original schema ('${originalSchema.name()}') and has no default value.",
            )
          }
      }
    }
    newStruct
  }

  /** Adapts an Array recursively. */
  private def adaptArray(
    originalList:   java.util.List[_],
    originalSchema: Schema,
    targetSchema:   Schema,
  ): java.util.List[Any] = {
    val targetElementSchema   = targetSchema.valueSchema()
    val originalElementSchema = originalSchema.valueSchema()

    val newList = new java.util.ArrayList[Any](originalList.size())
    originalList.asScala.foreach { originalElement =>
      val adaptedElement = adaptValue(originalElement, originalElementSchema, targetElementSchema) // Recursive call
      newList.add(adaptedElement)
    }
    newList
  }

  /** Adapts a Map recursively (values only, keys assumed compatible). */
  private def adaptMap(
    originalMap:    java.util.Map[_, _],
    originalSchema: Schema,
    targetSchema:   Schema,
  ): java.util.Map[Any, Any] = {
    val targetKeySchema     = targetSchema.keySchema()
    val targetValueSchema   = targetSchema.valueSchema()
    val originalKeySchema   = originalSchema.keySchema()
    val originalValueSchema = originalSchema.valueSchema()

    // Basic check for key schema compatibility (often String). More complex key adaptation is not handled.
    if (!originalKeySchema.equals(targetKeySchema) && originalKeySchema.`type`() != targetKeySchema.`type`()) {
      logger.warn(
        s"Map key schema mismatch during adaptation (adaptation not attempted): original=${originalKeySchema}, target=${targetKeySchema}",
      )
      // Could throw exception if strict key schema matching is required
    }

    val newMap = new java.util.HashMap[Any, Any](originalMap.size())
    originalMap.asScala.foreach {
      case (originalKey, originalValue) =>
        // Key adaptation: Assuming keys are compatible and don't need adaptation for simplicity.
        val adaptedKey = originalKey

        // Value adaptation: Recursively adapt the value
        val adaptedValue = adaptValue(originalValue, originalValueSchema, targetValueSchema) // Recursive call

        newMap.put(adaptedKey, adaptedValue)
    }
    newMap
  }

  /**
   * Determines if newSchema is newer than currentSchema based on version.
   * Assumes Schema.version() returns a comparable integer where higher means newer.
   * @param newSchema The schema to check (assumed non-null)
   * @param currentSchema The current schema we have stored (assumed non-null)
   * @return true if newSchema has a higher version, false otherwise.
   */
  private def isNewerSchema(newSchema: Schema, currentSchema: Schema): Boolean =
    // Ensure both schemas have versions; handle potential null versions if necessary.
    // If version() can return null or comparison isn't straightforward, add checks here.
    // Assuming version() returns non-null Int for simplicity based on context.
    Option(newSchema.version()).exists { newVer =>
      Option(currentSchema.version()).exists { currentVer =>
        newVer > currentVer
      }
    }

  /**
   * Detects if a schema represents an Avro union type.
   * Confluent's AvroConverter represents non-null Avro unions as STRUCTs
   * with the schema name "io.confluent.connect.avro.Union".
   */
  private def isUnionSchema(schema: Schema): Boolean =
    schema.`type`() == Schema.Type.STRUCT &&
      schema.name() != null &&
      schema.name() == ConfluentAvroUnionSchemaName

  /**
   * Promotes a simple value to a union struct by finding the matching branch.
   * For example, promotes an enum string "PENDING" to a union struct with the
   * enum branch populated and other branches set to null.
   *
   * @param value The original simple value (e.g., String for an enum)
   * @param originalSchema The schema of the original value
   * @param targetUnionSchema The target union schema (must be a union STRUCT)
   * @return A Struct representing the union with the appropriate branch populated
   */
  private def promoteToUnion(value: Any, originalSchema: Schema, targetUnionSchema: Schema): Struct = {
    val unionStruct = new Struct(targetUnionSchema)

    // Find the matching branch in the union by comparing schema names or types
    // Priority: name matches take precedence over type matches to correctly handle
    // cases like [string, enum] where enum (STRING type with name) should match
    // the enum branch, not the plain string branch
    val fields = targetUnionSchema.fields().asScala

    // First, try to find a field with matching schema name (for named types like enums)
    val nameMatchingField = Option(originalSchema.name()).flatMap { origName =>
      fields.find { field =>
        Option(field.schema().name()).contains(origName)
      }
    }

    // Fall back to type matching only if no name match exists
    val matchingField = nameMatchingField.orElse {
      fields.find { field =>
        field.schema().`type`() == originalSchema.`type`()
      }
    }

    matchingField match {
      case Some(field) =>
        // Recursively adapt the value to the target branch schema
        val adaptedValue = adaptValue(value, originalSchema, field.schema())
        // Set the matching branch to the adapted value, all other branches are null by default
        targetUnionSchema.fields().asScala.foreach { f =>
          if (f.name() == field.name()) {
            unionStruct.put(f, adaptedValue)
          } else {
            unionStruct.put(f, null)
          }
        }
        unionStruct
      case None =>
        throw new DataException(
          s"Cannot promote value to union: No matching branch found for schema '${originalSchema.name()}' " +
            s"(type: ${originalSchema.`type`()}) in union schema '${targetUnionSchema.name()}'",
        )
    }
  }

  /**
   * Extracts the active value from a union struct.
   * A union struct has one non-null field (the active branch) and other fields set to null.
   *
   * @param unionStruct The union struct to extract from
   * @param unionSchema The schema of the union struct
   * @param targetSchema The target simple schema to extract to
   * @return The extracted value from the active branch
   */
  private def extractFromUnion(unionStruct: Struct, unionSchema: Schema, targetSchema: Schema): Any = {
    // Find the non-null field (active branch)
    val activeField = unionSchema.fields().asScala.find { field =>
      unionStruct.get(field) != null
    }

    activeField match {
      case Some(field) =>
        val value       = unionStruct.get(field)
        val fieldSchema = field.schema()

        // Verify the extracted value is compatible with target schema
        val isCompatible = {
          val nameMatch =
            Option(targetSchema.name()).exists(targetName => Option(fieldSchema.name()).contains(targetName))
          val typeMatch = fieldSchema.`type`() == targetSchema.`type`()
          nameMatch || typeMatch
        }

        if (isCompatible) {
          // Recursively adapt the value to the target schema
          adaptValue(value, fieldSchema, targetSchema)
        } else {
          throw new DataException(
            s"Cannot extract from union: Active branch '${field.name()}' (type: ${fieldSchema.`type`()}) " +
              s"is not compatible with target schema '${targetSchema.name()}' (type: ${targetSchema.`type`()})",
          )
        }
      case None =>
        // All fields are null - return null if target allows it
        if (targetSchema.isOptional || targetSchema.defaultValue() != null) {
          targetSchema.defaultValue()
        } else {
          throw new DataException(
            s"Cannot extract from union: All branches are null but target schema '${targetSchema.name()}' is required",
          )
        }
    }
  }

}

object AttachLatestSchemaOptimizer {

  /** Schema name used by Confluent's AvroConverter for union types */
  private[optimization] val ConfluentAvroUnionSchemaName = "io.confluent.connect.avro.Union"
}
