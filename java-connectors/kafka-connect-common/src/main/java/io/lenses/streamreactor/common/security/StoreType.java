/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.common.security;

import cyclops.control.Either;
import cyclops.control.Try;
import io.lenses.streamreactor.common.exception.SecuritySetupException;
import lombok.Getter;

@Getter
public enum StoreType {

  JKS("JKS"),
  PKCS12("PKCS12");

  private final String type;

  StoreType(String type) {
    this.type = type;
  }

  public static Either<SecuritySetupException, StoreType> valueOfCaseInsensitive(String storeType) {
    return Try
        .withCatch(() -> StoreType.valueOf(storeType.toUpperCase()))
        .toEither()
        .mapLeft(ex -> new SecuritySetupException(String.format("Unable to retrieve Store type %s", storeType), ex));
  }

  public static final StoreType DEFAULT_STORE_TYPE = StoreType.JKS;

}
