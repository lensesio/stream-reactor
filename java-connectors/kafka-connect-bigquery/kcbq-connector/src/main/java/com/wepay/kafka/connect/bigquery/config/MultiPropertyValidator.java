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

package com.wepay.kafka.connect.bigquery.config;

import org.apache.kafka.common.config.ConfigValue;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public abstract class MultiPropertyValidator<Config> {

  private final String propertyName;

  protected MultiPropertyValidator(String propertyName) {
    this.propertyName = propertyName;
  }

  public String propertyName() {
    return propertyName;
  }

  public Optional<String> validate(ConfigValue value, Config config, Map<String, ConfigValue> valuesByName) {
    // Only perform follow-up validation if the property doesn't already have an error associated with it
    if (!value.errorMessages().isEmpty()) {
      return Optional.empty();
    }

    boolean dependentsAreValid = dependents().stream()
        .map(valuesByName::get)
        .filter(Objects::nonNull)
        .map(ConfigValue::errorMessages)
        .allMatch(List::isEmpty);
    // Also ensure that all of the other properties that the validation for this one depends on don't already have errors
    if (!dependentsAreValid) {
      return Optional.empty();
    }

    try {
      return doValidate(config);
    } catch (RuntimeException e) {
      return Optional.of(
          "An unexpected error occurred during validation"
              + (e.getMessage() != null ? ": " + e.getMessage() : "")
      );
    }
  }

  protected abstract Collection<String> dependents();
  protected abstract Optional<String> doValidate(Config config);
}
