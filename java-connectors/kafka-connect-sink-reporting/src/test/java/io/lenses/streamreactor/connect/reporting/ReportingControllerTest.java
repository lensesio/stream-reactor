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
package io.lenses.streamreactor.connect.reporting;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import cyclops.control.Option;
import io.lenses.streamreactor.connect.reporting.model.ReportingRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ReportingControllerTest {

  @Mock
  private ReportSender<TestConnectorSpecificRecordDataData> mockReportSender;
  @Mock
  private ReportingRecord<TestConnectorSpecificRecordDataData> mockReportingRecord;

  private ReportingController<TestConnectorSpecificRecordDataData> reportingController;

  @BeforeEach
  public void setUp() {
    reportingController =
        new ReportingController<>(
            Option.of(mockReportSender)
        );
  }

  @Test
  void testEnqueue() {
    reportingController.enqueue(mockReportingRecord);

    verify(mockReportSender, times(1)).enqueue(mockReportingRecord);
  }

  @Test
  void testStart() {
    reportingController.start();

    verify(mockReportSender, times(1)).start();
  }

  @Test
  void testClose() {
    reportingController.close();

    verify(mockReportSender, times(1)).close();
  }

}
