package com.wepay.kafka.connect.bigquery.it;

/*
 * Copyright 2016 WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import static com.google.cloud.bigquery.LegacySQLTypeName.BOOLEAN;
import static com.google.cloud.bigquery.LegacySQLTypeName.BYTES;
import static com.google.cloud.bigquery.LegacySQLTypeName.DATE;
import static com.google.cloud.bigquery.LegacySQLTypeName.FLOAT;
import static com.google.cloud.bigquery.LegacySQLTypeName.INTEGER;
import static com.google.cloud.bigquery.LegacySQLTypeName.STRING;
import static com.google.cloud.bigquery.LegacySQLTypeName.TIMESTAMP;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;

import com.wepay.kafka.connect.bigquery.BigQueryHelper;
import com.wepay.kafka.connect.bigquery.exception.SinkConfigConnectException;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.InputStream;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class BigQueryConnectorIntegrationTest {
  public static final String TEST_PROPERTIES_FILENAME = "/test.properties";
  public static final String KEYFILE_PROPERTY = "keyfile";
  public static final String PROJECT_PROPERTY = "project";
  public static final String DATASET_PROPERTY = "dataset";

  private static String keyfile;
  private static String project;
  private static String dataset;

  private static BigQuery bigQuery;

  @BeforeClass
  public static void initialize() throws Exception {
    initializeTestProperties();
    initializeBigQuery();
  }

  private static void initializeTestProperties() throws Exception {
    try (InputStream propertiesFile =
        BigQueryConnectorIntegrationTest.class.getResourceAsStream(TEST_PROPERTIES_FILENAME)) {
      if (propertiesFile == null) {
        throw new FileNotFoundException(
            "Resource file '" + TEST_PROPERTIES_FILENAME
            + "' must be provided in order to run integration tests"
        );
      }

      Properties properties = new Properties();
      properties.load(propertiesFile);

      keyfile = properties.getProperty(KEYFILE_PROPERTY);
      if (keyfile == null) {
        throw new SinkConfigConnectException(
            "'" + KEYFILE_PROPERTY
            + "' property must be specified in test properties file"
        );
      }

      project = properties.getProperty(PROJECT_PROPERTY);
      if (project == null) {
        throw new SinkConfigConnectException(
            "'" + PROJECT_PROPERTY
            + "' property must be specified in test properties file"
        );
      }

      dataset = properties.getProperty(DATASET_PROPERTY);
      if (dataset == null) {
        throw new SinkConfigConnectException(
            "'" + DATASET_PROPERTY
            + "' property must be specified in test properties file"
        );
      }
    }
  }

  private static void initializeBigQuery() throws Exception {
    bigQuery = new BigQueryHelper().connect(project, keyfile);
  }

  private static List<Byte> boxByteArray(byte[] bytes) {
    Byte[] result = new Byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      result[i] = bytes[i];
    }
    return Arrays.asList(result);
  }

  private Object convertField(Field fieldSchema, FieldValue field) {
    if (field.isNull()) {
      return null;
    }
    switch (field.getAttribute()) {
      case PRIMITIVE:
        if (fieldSchema.getType().equals(BOOLEAN)) {
          return field.getBooleanValue();
        } else if (fieldSchema.getType().equals(BYTES)) {
          // Do this in order for assertEquals() to work when this is an element of two compared
          // lists
          return boxByteArray(field.getBytesValue());
        } else if (fieldSchema.getType().equals(DATE)) {
          DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
          long millisecondsSinceEpoch = LocalDate.parse(field.getStringValue(), dateFormatter)
                  .atStartOfDay(ZoneOffset.UTC)
                  .toInstant()
                  .toEpochMilli();
          return millisecondsSinceEpoch;
        } else if (fieldSchema.getType().equals(FLOAT)) {
          return field.getDoubleValue();
        } else if (fieldSchema.getType().equals(INTEGER)) {
          return field.getLongValue();
        } else if (fieldSchema.getType().equals(STRING)) {
          return field.getStringValue();
        } else if (fieldSchema.getType().equals(TIMESTAMP)) {
          return field.getTimestampValue();
        } else {
          throw new RuntimeException("Cannot convert primitive field type "
                  + fieldSchema.getType());
        }
      case REPEATED:
        List<Object> result = new ArrayList<>();
        for (FieldValue arrayField : field.getRepeatedValue()) {
          result.add(convertField(fieldSchema, arrayField));
        }
        return result;
      case RECORD:
        List<Field> recordSchemas = fieldSchema.getSubFields();
        List<FieldValue> recordFields = field.getRecordValue();
        return convertRow(recordSchemas, recordFields);
      default:
        throw new RuntimeException("Unknown field attribute: " + field.getAttribute());
    }
  }

  private List<Object> convertRow(List<Field> rowSchema, List<FieldValue> row) {
    List<Object> result = new ArrayList<>();
    assert (rowSchema.size() == row.size());

    for (int i = 0; i < rowSchema.size(); i++) {
      result.add(convertField(rowSchema.get(i), row.get(i)));
    }

    return result;
  }

  private List<List<Object>> readAllRows(String tableName) {
    Table table = bigQuery.getTable(dataset, tableName);
    Schema schema = table.getDefinition().getSchema();

    List<List<Object>> rows = new ArrayList<>();
    TableResult tableResult = table.list();

    while (tableResult != null) {
      Iterable<FieldValueList> fieldValueLists = tableResult.iterateAll();
      for (FieldValueList fieldValueList : fieldValueLists) {
        rows.add(convertRow(schema.getFields(), fieldValueList));
      }
      tableResult = tableResult.getNextPage();
    }
    return rows;
  }

  @Test
  public void testNull() {
    List<List<Object>> expectedRows = new ArrayList<>();

    // {"row":1,"f1":"Required string","f2":null,"f3":{"int":42},"f4":{"boolean":false}}
    expectedRows.add(Arrays.asList(1L, "Required string", null, 42L, false));
    // {"row":2,"f1":"Required string","f2":{"string":"Optional string"},"f3":{"int":89},"f4":null}
    expectedRows.add(Arrays.asList(2L, "Required string", "Optional string", 89L, null));
    // {"row":3,"f1":"Required string","f2":null,"f3":null,"f4":{"boolean":true}}
    expectedRows.add(Arrays.asList(3L, "Required string", null, null, true));
    // {"row":4,"f1":"Required string","f2":{"string":"Optional string"},"f3":null,"f4":null}
    expectedRows.add(Arrays.asList(4L, "Required string", "Optional string", null, null));

    testRows(expectedRows, readAllRows("kcbq_test_nulls"));
  }

  @Test
  public void testMatryoshka() {
    List<List<Object>> expectedRows = new ArrayList<>();

    /* { "row": 1,
          "middle":
            { "middle_array": [42.0, 42.42, 42.4242],
              "inner":
                { "inner_int": 42,
                  "inner_string": "42"
                }
            },
          "inner":
            { "inner_int": -42,
              "inner_string": "-42"
            }
        } */
    expectedRows.add(Arrays.asList(
        1L,
        Arrays.asList(
            Arrays.asList(42.0, 42.42, 42.4242),
            Arrays.asList(
                42L,
                "42"
            )
        ),
        Arrays.asList(
            -42L,
            "-42"
        )
    ));

    testRows(expectedRows, readAllRows("kcbq_test_matryoshka_dolls"));
  }

  @Test
  public void testPrimitives() {
    List<List<Object>> expectedRows = new ArrayList<>();

    /* { "row": 1,
          "null_prim": null,
          "boolean_prim": false,
          "int_prim": 4242,
          "long_prim": 42424242424242,
          "float_prim": 42.42,
          "double_prim": 42424242.42424242,
          "string_prim": "forty-two",
          "bytes_prim": "\u0000\u000f\u001e\u002d\u003c\u004b\u005a\u0069\u0078"
        } */
    expectedRows.add(Arrays.asList(
        1L,
        null,
        false,
        4242L,
        42424242424242L,
        42.42,
        42424242.42424242,
        "forty-two",
        boxByteArray(new byte[] { 0x0, 0xf, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78 })
    ));

    testRows(expectedRows, readAllRows("kcbq_test_primitives"));
  }

  @Test
  public void testLogicalTypes() {
    List<List<Object>> expectedRows = new ArrayList<>();

    // {"row": 1, "timestamp-test": 0, "date-test": 0}
    expectedRows.add(Arrays.asList(1L, 0L, 0L));
    // {"row": 2, "timestamp-test": 42000000, "date-test": 4200}
    expectedRows.add(Arrays.asList(2L, 42000000000L, 362880000000L));
    // {"row": 3, "timestamp-test": 1468275102000, "date-test": 16993}
    expectedRows.add(Arrays.asList(3L, 1468275102000000L, 1468195200000L));

    testRows(expectedRows, readAllRows("kcbq_test_logical_types"));
  }

  @Test
  public void testGCSLoad() {
    List<List<Object>> expectedRows = new ArrayList<>();

    /* {"row":1,
        "null_prim":null,
        "boolean_prim":false,
        "int_prim":4242,
        "long_prim":42424242424242,
        "float_prim":42.42,
        "double_prim":42424242.42424242,
        "string_prim":"forty-two",
        "bytes_prim":"\u0000\u000f\u001e\u002d\u003c\u004b\u005a\u0069\u0078"}
     */
    expectedRows.add(Arrays.asList(
        1L,
        null,
        false,
        4242L,
        42424242424242L,
        42.42,
        42424242.42424242,
        "forty-two",
        boxByteArray(new byte[] { 0x0, 0xf, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78 })
    ));
    /* {"row":2,
        "null_prim":{"int":5},
        "boolean_prim":true,
        "int_prim":4354,
        "long_prim":435443544354,
        "float_prim":43.54,
        "double_prim":435443.544354,
        "string_prim":"forty-three",
        "bytes_prim":"\u0000\u000f\u001e\u002d\u003c\u004b\u005a\u0069\u0078"}
    */
    expectedRows.add(Arrays.asList(
        2L,
        5L,
        true,
        4354L,
        435443544354L,
        43.54,
        435443.544354,
        "forty-three",
        boxByteArray(new byte[] { 0x0, 0xf, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78 })
    ));
    /* {"row":3,
        "null_prim":{"int":8},
        "boolean_prim":false,
        "int_prim":1993,
        "long_prim":199319931993,
        "float_prim":19.93,
        "double_prim":199319.931993,
        "string_prim":"nineteen",
        "bytes_prim":"\u0000\u000f\u001e\u002d\u003c\u004b\u005a\u0069\u0078"}
    */
    expectedRows.add(Arrays.asList(
        3L,
        8L,
        false,
        1993L,
        199319931993L,
        19.93,
        199319.931993,
        "nineteen",
        boxByteArray(new byte[] { 0x0, 0xf, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x69, 0x78 })
    ));

    testRows(expectedRows, readAllRows("kcbq_test_gcs_load"));
  }

  private void testRows(
      List<List<Object>> expectedRows,
      List<List<Object>> testRows) {
    assertEquals("Number of expected rows should match", expectedRows.size(), testRows.size());

    for (List<Object> testRow : testRows) {
      int rowNumber = (int) (((Long) testRow.get(0)).longValue());
      List<Object> expectedRow = expectedRows.get(rowNumber - 1);
      assertEquals(
          "Row " + rowNumber + " (if these look identical, it's probably a type mismatch)",
          expectedRow,
          testRow
      );
    }
  }
}
