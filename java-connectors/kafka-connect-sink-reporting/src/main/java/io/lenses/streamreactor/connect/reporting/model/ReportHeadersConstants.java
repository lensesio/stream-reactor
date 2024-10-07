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
package io.lenses.streamreactor.connect.reporting.model;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ReportHeadersConstants {

  public static final String INPUT_OFFSET = "input_offset";
  public static final String INPUT_TIMESTAMP = "input_timestamp";
  public static final String INPUT_PARTITION = "input_partition";
  public static final String INPUT_TOPIC = "input_topic";
  public static final String INPUT_KEY = "input_key";
  public static final String INPUT_PAYLOAD = "input_payload";
  public static final String ERROR = "error_message";
  public static final String RESPONSE_CONTENT = "response_content";
  public static final String RESPONSE_STATUS = "response_status_code";

}
