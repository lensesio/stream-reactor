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

/**
 * This marker interface represents data specific to a record for a
 * certain connector. Implementations of this interface should
 * provide the necessary details that are specific to a particular
 * connector.
 * This sits inside a ReportingRecord providing the data that is
 * bespoke for a given connector.
 * 
 * @see io.lenses.streamreactor.connect.reporting.model.ReportingRecord
 */
public interface ConnectorSpecificRecordData {
}
