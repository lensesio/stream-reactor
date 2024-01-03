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
package io.lenses.kcql;

public class Tag {
  private final String key;
  private final String value;
  private final TagType type;

  public Tag(String key, String value, TagType type) {
    this.key = key;
    this.value = value;
    this.type = type;
  }

  public Tag(String key) {
    this.key = key;
    this.value = null;
    this.type = TagType.DEFAULT;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public TagType getType() {
    return type;
  }

  public enum TagType {
    DEFAULT,
    ALIAS,
    CONSTANT
  }
}
