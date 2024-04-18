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
package io.lenses.streamreactor.common.errors;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

public enum ErrorPolicyEnum {
  NOOP(new NoopErrorPolicy()),
  THROW(new ThrowErrorPolicy()),
  RETRY(new RetryErrorPolicy());

  private static final Map<String, ErrorPolicy> errorPolicyByName =
      Arrays.stream(ErrorPolicyEnum.values())
          .collect(toMap(Enum::name, ErrorPolicyEnum::getErrorPolicy));

  @Getter
  private final ErrorPolicy errorPolicy;

  ErrorPolicyEnum(ErrorPolicy errorPolicy) {
    this.errorPolicy = errorPolicy;
  }

  public static ErrorPolicy byName(String name) {
    Optional<ErrorPolicy> policyByName = ofNullable(errorPolicyByName.get(name));
    return policyByName.orElseThrow(
        () -> new RuntimeException("Couldn't find ErrorPolicy by name: " + name));
  }
}
