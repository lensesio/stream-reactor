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
package io.lenses.streamreactor.connect.azure.servicebus.source;

import org.apache.kafka.connect.source.SourceRecord;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Class to hold mapping between original ServiceBus Message and its translated version, as well as information on
 * which Receiver needs to complete (commit) it.
 */
@Getter
@AllArgsConstructor
public class ServiceBusMessageHolder {

  private final ServiceBusReceivedMessage originalRecord;
  private final SourceRecord translatedRecord;
  private final String receiverId;
}
