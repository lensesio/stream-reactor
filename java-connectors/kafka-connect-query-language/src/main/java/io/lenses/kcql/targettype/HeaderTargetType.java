/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.kcql.targettype;

import lombok.Data;

/**
 * The HeaderTargetType class represents a header target.
 * This type of target is used when the target string starts with "_header." followed by a path.
 */
@Data
public class HeaderTargetType implements TargetType {

  /**
   * The name of the header.
   */
  private final String name;
}
