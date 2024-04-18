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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.ConnectException;

@Slf4j
public class ThrowErrorPolicy implements ErrorPolicy {
  @Override
  public void handle(Throwable throwable, Boolean sink, Integer retryCount) {
    throw new ConnectException(throwable);
  }
}
