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
package io.lenses.kcql;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Field {

  private final String name;
  private final String alias;
  private final FieldType fieldType;
  private final List<String> parentFields;

  public Field(String name, String alias, FieldType fieldType) {
    this(name, alias, fieldType, null);
  }

  public Field(String name, FieldType fieldType, List<String> parents) {
    this(name, name, fieldType, parents);
  }

  public Field(String name, String alias, FieldType fieldType, List<String> parents) {
    if (name == null || name.trim().isEmpty()) {
      throw new IllegalArgumentException(String.format("field is not valid:<%s>", String.valueOf(name)));
    }
    if (alias == null || alias.trim().isEmpty()) {
      throw new IllegalArgumentException(String.format("alias is not valid:<%s>", String.valueOf(alias)));
    }
    this.name = name;
    this.alias = alias;
    this.fieldType = fieldType;
    this.parentFields = parents;
  }

  public String getName() {
    return name;
  }

  public String getAlias() {
    return alias;
  }

  public boolean hasParents() {
    return parentFields != null;
  }

  public List<String> getParentFields() {
    if (parentFields == null)
      return null;
    return new ArrayList<>(parentFields);
  }

  public String toString() {
    if (parentFields == null || parentFields.isEmpty())
      return name;
    StringBuilder sb = new StringBuilder(parentFields.get(0));
    for (int i = 1; i < parentFields.size(); ++i) {
      sb.append(".");
      sb.append(parentFields.get(i));
    }
    sb.append(".");
    sb.append(name);
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    Field field = (Field) o;

    if (!Objects.equals(name, field.name))
      return false;
    if (!Objects.equals(alias, field.alias))
      return false;
    if (fieldType != field.fieldType)
      return false;
    return Objects.equals(parentFields, field.parentFields);
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (alias != null ? alias.hashCode() : 0);
    result = 31 * result + (fieldType != null ? fieldType.hashCode() : 0);
    result = 31 * result + (parentFields != null ? parentFields.hashCode() : 0);
    return result;
  }

  public static Field from(String name, List<String> parents) {
    return from(name, null, parents);
  }

  public static Field from(String name, String alias, List<String> parents) {
    if (parents != null) {
      if (UNDERSCORE.equals(parents.get(0))) {
        switch (name.toLowerCase()) {

          case TOPIC:
            if (alias != null) {
              return new Field(TOPIC, alias, FieldType.TOPIC);
            }
            return new Field(TOPIC, TOPIC, FieldType.TOPIC);

          case OFFSET:
            if (alias != null) {
              return new Field(OFFSET, alias, FieldType.OFFSET);
            }

            return new Field(OFFSET, OFFSET, FieldType.OFFSET);

          case TIMESTAMP:
            if (alias != null) {
              return new Field(TIMESTAMP, alias, FieldType.TIMESTAMP);
            }
            return new Field(TIMESTAMP, TIMESTAMP, FieldType.TIMESTAMP);

          case PARTITION:
            if (alias != null) {
              return new Field(PARTITION, alias, FieldType.PARTITION);
            }
            return new Field(PARTITION, PARTITION, FieldType.PARTITION);

          default:
            if (parents.size() <= 1 || !"key".equals(parents.get(1).toLowerCase())) {
              throw new IllegalArgumentException(String.format(
                  "Invalid syntax. '_' needs to be followed by: key,%s,%s,%s,%s", TOPIC, PARTITION, TIMESTAMP, OFFSET));
            }

            if (parents.size() <= 2) {
              if (alias != null) {
                if ("*".equals(name)) {
                  throw new IllegalArgumentException("You can't alias '*'.");
                }
                return new Field(name, alias, FieldType.KEY, null);
              }
              return new Field(name, FieldType.KEY, null);
            }

            List<String> parentsCopy = new ArrayList<>();
            for (int i = 2; i < parents.size(); ++i) {
              parentsCopy.add(parents.get(i));
            }
            if (alias != null) {
              return new Field(name, alias, FieldType.KEY, parentsCopy);
            }
            return new Field(name, FieldType.KEY, parentsCopy);

        }
      }
    }
    List<String> parentsCopy = null;
    if (parents != null) {
      parentsCopy = new ArrayList<>(parents);
    }
    if (alias != null) {
      return new Field(name, alias, FieldType.VALUE, parentsCopy);
    }
    return new Field(name, FieldType.VALUE, parentsCopy);
  }

  private static final String UNDERSCORE = "_";
  private static final String OFFSET = "offset";
  private static final String TOPIC = "topic";
  private static final String PARTITION = "partition";
  private static final String TIMESTAMP = "timestamp";

  public FieldType getFieldType() {
    return fieldType;
  }
}
