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
package io.lenses.streamreactor.connect.cloud.common.sink.optimization

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
        targetSchema.`type`() match {
          case Schema.Type.STRUCT =>
            value match {
              case s: Struct => adaptStruct(s, originalSchema, targetSchema)
              case other =>
                throw new DataException(s"Expected Struct for schema '${targetSchema.name()}', found ${other.getClass}")
            }
          case Schema.Type.ARRAY =>
            if (originalSchema.`type`() != Schema.Type.ARRAY)
              throw new DataException(
                s"Schema mismatch: Expected ARRAY but original schema is ${originalSchema.`type`()}",
              )
            value match {
              case l: java.util.List[_] => adaptArray(l, originalSchema, targetSchema)
              case other =>
                throw new DataException(s"Expected List for schema '${targetSchema.name()}', found ${other.getClass}")
            }
          case Schema.Type.MAP =>
            if (originalSchema.`type`() != Schema.Type.MAP)
              throw new DataException(
                s"Schema mismatch: Expected MAP but original schema is ${originalSchema.`type`()}",
              )
            value match {
              case m: java.util.Map[_, _] => adaptMap(m, originalSchema, targetSchema)
              case other =>
                throw new DataException(s"Expected Map for schema '${targetSchema.name()}', found ${other.getClass}")
            }
          case _ => value
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

}
