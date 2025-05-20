/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.common.validation.validators;

import io.lenses.kcql.Kcql;
import java.util.List;

/**
 * Validator that checks how all {@link Kcql}s interact together against some rules.
 */
public interface AllKcqlValidator {

  /**
   * Validates List of KCQL statements against specific rule.
   * 
   * @param kcqls {@link Kcql} list to be validated
   * @return List of errors (can be empty if Kcqls are valid)
   */
  List<String> validate(List<Kcql> kcqls);

}
