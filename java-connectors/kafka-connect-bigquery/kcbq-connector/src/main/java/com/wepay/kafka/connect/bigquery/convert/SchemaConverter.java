/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.convert;

import org.apache.kafka.connect.data.Schema;

/**
 * Interface for converting from a {@link Schema Kafka Connect Schema} to some other kind of schema.
 * @param <S> The kind of schema to convert to.
 */
public interface SchemaConverter<S> {
  /**
   * @param schema The schema to convert.
   * @return The converted schema.
   */
  S convertSchema(Schema schema);
}
