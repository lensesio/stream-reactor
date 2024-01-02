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
package io.lenses.streamreactor.connect.cloud.common.config

import cats.data.ValidatedNel
import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxTuple2Semigroupal
import cats.implicits.catsSyntaxValidatedId
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties
import org.apache.kafka.common.config.ConfigException

/**
  * Flags for enabling/disabling storage of various fields in a Kafka message.
  *
  * @param envelope - If enabled it stores the envelope of the Kafka message
  * @param key - If enabled it stores the keys of the Kafka message
  * @param value - If enabled it stores the values of the Kafka message
  * @param metadata - If enabled it stores the metadata of the Kafka message
  * @param headers - If enabled it stores the headers of the Kafka message
  */
case class DataStorageSettings(
  envelope: Boolean,
  key:      Boolean,
  value:    Boolean,
  metadata: Boolean,
  headers:  Boolean,
) {
  def hasEnvelope: Boolean = envelope
}

object DataStorageSettings {

  val StoreEnvelopeKey  = "store.envelope"
  private val KeyPrefix = "store.envelope.fields"
  val StoreKeyKey       = s"$KeyPrefix.key"
  val StoreHeadersKey   = s"$KeyPrefix.headers"
  val StoreValueKey     = s"$KeyPrefix.value"
  val StoreMetadataKey  = s"$KeyPrefix.metadata"
  val AllEnvelopeFields: Seq[String] = List(
    PropsKeyEnum.StoreEnvelopeKey,
    PropsKeyEnum.StoreEnvelopeValue,
    PropsKeyEnum.StoreEnvelopeHeaders,
    PropsKeyEnum.StoreEnvelopeMetadata,
  ).map(_.entryName)
  private val DefaultEnvelopeValue = false
  private val DefaultFieldsValue   = true

  val Default: DataStorageSettings = DataStorageSettings(
    envelope = DefaultEnvelopeValue,
    key      = DefaultFieldsValue,
    value    = DefaultFieldsValue,
    metadata = DefaultFieldsValue,
    headers  = DefaultFieldsValue,
  )

  def disabled: DataStorageSettings = Default
  def enabled: DataStorageSettings =
    DataStorageSettings(envelope = true, key = true, value = true, metadata = true, headers = true)

  def from(properties: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]): Either[ConfigException, DataStorageSettings] =
    for {
      envelope <- properties.getBooleanOrDefault(PropsKeyEnum.StoreEnvelope, DefaultEnvelopeValue)
      key      <- properties.getBooleanOrDefault(PropsKeyEnum.StoreEnvelopeKey, DefaultFieldsValue)
      metadata <- properties.getBooleanOrDefault(PropsKeyEnum.StoreEnvelopeMetadata, DefaultFieldsValue)
      headers  <- properties.getBooleanOrDefault(PropsKeyEnum.StoreEnvelopeHeaders, DefaultFieldsValue)
      value    <- properties.getBooleanOrDefault(PropsKeyEnum.StoreEnvelopeValue, DefaultFieldsValue)
      result <- if (!envelope) {
        //if no envelope default
        Default.asRight[ConfigException]
      } else {
        val setting = DataStorageSettings(
          envelope = envelope,
          key      = key,
          value    = value,
          metadata = metadata,
          headers  = headers,
        )

        (
          validateEnvelopeIsTrueAndAtLeastOneField(setting),
          validateEnvelopeIsTrueAndAllFieldsSpecified(envelope, properties),
        ).mapN((_, _) => setting).leftMap(errors => new ConfigException(errors.toList.mkString(", "))).toEither
      }
    } yield result

  /**
    * Validates that the envelope is true and at least one field is true
    *
    * @param setting The setting to validate
    * @return An error if the envelope is true and all the fields are false, otherwise unit
    */
  private def validateEnvelopeIsTrueAndAtLeastOneField(setting: DataStorageSettings): ValidatedNel[String, Unit] =
    if (setting.envelope) {
      if (setting.key || setting.value || setting.metadata || setting.headers) {
        ().validNel
      } else {
        s"If ${DataStorageSettings.StoreEnvelopeKey} is set to true then at least one of ${AllEnvelopeFields.mkString("[", ",", "]")} must be set to true.".invalidNel
      }
    } else ().validNel

  /**
    * Validates that if the envelope is set to true then all the fields are missing or all fields are specified.
    * This is meant to drive explicit configuration of the fields, and avoid selective fields configuration.
    * @param properties The properties to validate
    * @return An error if the envelope is true and not all fields are specified, otherwise unit
    */
  private def validateEnvelopeIsTrueAndAllFieldsSpecified(
    envelope:   Boolean,
    properties: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type],
  ): ValidatedNel[String, Unit] =
    if (!envelope) ().validNel
    else {
      val key      = properties.getOptionalBoolean(PropsKeyEnum.StoreEnvelopeKey)
      val value    = properties.getOptionalBoolean(PropsKeyEnum.StoreEnvelopeValue)
      val metadata = properties.getOptionalBoolean(PropsKeyEnum.StoreEnvelopeMetadata)
      val headers  = properties.getOptionalBoolean(PropsKeyEnum.StoreEnvelopeHeaders)
      val allDefined =
        key.exists(_.isDefined) && value.exists(_.isDefined) && metadata.exists(_.isDefined) && headers.exists(
          _.isDefined,
        )
      val allNotDefined =
        key.exists(_.isEmpty) && value.exists(_.isEmpty) && metadata.exists(_.isEmpty) && headers.exists(_.isEmpty)
      if (allDefined || allNotDefined) ().validNel
      else
        s"If ${DataStorageSettings.StoreEnvelopeKey} is set to true, then setting selective fields is not allowed. Either set them all or leave them out, they default to true.".invalidNel
    }

}
