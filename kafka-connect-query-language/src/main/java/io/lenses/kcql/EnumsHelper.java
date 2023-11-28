/*
 * Copyright 2017-2023 Lenses.io Ltd
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

public class EnumsHelper {
  public static <T extends Enum<T>> String mkString(T[] values) {
    StringBuilder sb = new StringBuilder(values[0].toString());
    for (int i = 1; i < values.length; ++i) {
      sb.append(",");
      sb.append(values[i].toString());
    }
    return sb.toString();
  }

}
