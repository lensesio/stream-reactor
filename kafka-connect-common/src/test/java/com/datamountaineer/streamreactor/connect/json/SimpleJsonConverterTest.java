/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.json;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

public class SimpleJsonConverterTest {

  SimpleJsonConverter converter = new SimpleJsonConverter();

  // Schema types

  @Test
  public void booleanToJson() {
    JsonNode converted = converter.fromConnectData(Schema.BOOLEAN_SCHEMA, true);
    assertTrue(converted.booleanValue());
  }

  @Test
  public void byteToJson() {
    JsonNode converted = converter.fromConnectData(Schema.INT8_SCHEMA, (byte) 12);
    assertEquals(12, converted.intValue());
  }

  @Test
  public void shortToJson() {
    JsonNode converted = converter.fromConnectData(Schema.INT16_SCHEMA, (short) 12);
    assertEquals(12, converted.intValue());
  }

  @Test
  public void intToJson() {
    JsonNode converted = converter.fromConnectData(Schema.INT32_SCHEMA, 12);
    assertEquals(12, converted.intValue());
  }

  @Test
  public void longToJson() {
    JsonNode converted = converter.fromConnectData(Schema.INT64_SCHEMA, 4398046511104L);
    assertEquals(4398046511104L, converted.longValue());
  }

  @Test
  public void floatToJson() {
    JsonNode converted = converter.fromConnectData(Schema.FLOAT32_SCHEMA, 12.34f);
    assertEquals(12.34f, converted.floatValue(), 0.001);
  }

  @Test
  public void doubleToJson() {
    JsonNode converted = converter.fromConnectData(Schema.FLOAT64_SCHEMA, 12.34);
    assertEquals(12.34, converted.doubleValue(), 0.001);
  }

  @Test
  public void bytesToJson() throws IOException {
    JsonNode converted = converter.fromConnectData(Schema.BYTES_SCHEMA, "test-string".getBytes());
    assertEquals(ByteBuffer.wrap("test-string".getBytes()), ByteBuffer.wrap(converted.binaryValue()));
  }

  @Test
  public void stringToJson() {
    JsonNode converted = converter.fromConnectData(Schema.STRING_SCHEMA, "test-string");
    assertEquals("test-string", converted.textValue());
  }

  @Test
  public void arrayToJson() {
    Schema int32Array = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
    JsonNode converted = converter.fromConnectData(int32Array, Arrays.asList(1, 2, 3));
    assertEquals(JsonNodeFactory.instance.arrayNode().add(1).add(2).add(3), converted);
  }

  @Test
  public void mapToJsonStringKeys() {
    Schema stringIntMap = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
    Map<String, Integer> input = new HashMap<>();
    input.put("key1", 12);
    input.put("key2", 15);
    JsonNode converted = converter.fromConnectData(stringIntMap, input);
    assertEquals(JsonNodeFactory.instance.objectNode().put("key1", 12).put("key2", 15), converted);
  }

  @Test
  public void mapToJsonNonStringKeys() {
    Schema intIntMap = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build();
    Map<Integer, Integer> input = new HashMap<>();
    input.put(1, 12);
    input.put(2, 15);
    JsonNode converted = converter.fromConnectData(intIntMap, input);

    assertTrue(converted.isArray());
    ArrayNode payload = (ArrayNode) converted;
    assertEquals(2, payload.size());
    Set<JsonNode> payloadEntries = new HashSet<>();
    for (JsonNode elem : payload)
      payloadEntries.add(elem);
    assertEquals(new HashSet<>(Arrays.asList(JsonNodeFactory.instance.arrayNode().add(1).add(12),
            JsonNodeFactory.instance.arrayNode().add(2).add(15))),
            payloadEntries
    );
  }

  @Test
  public void structToJson() {
    Schema schema = SchemaBuilder.struct().field("field1", Schema.BOOLEAN_SCHEMA).field("field2", Schema.STRING_SCHEMA).field("field3", Schema.STRING_SCHEMA).field("field4", Schema.BOOLEAN_SCHEMA).build();
    Struct input = new Struct(schema).put("field1", true).put("field2", "string2").put("field3", "string3").put("field4", false);
    JsonNode converted = converter.fromConnectData(schema, input);

    assertEquals(JsonNodeFactory.instance.objectNode()
                    .put("field1", true)
                    .put("field2", "string2")
                    .put("field3", "string3")
                    .put("field4", false),
            converted);
  }


  @Test
  public void decimalToJson() {
    BigDecimal expectedDecimal = new BigDecimal(new BigInteger("156"), 2);
    JsonNode converted = converter.fromConnectData(Decimal.schema(2), expectedDecimal);
    assertEquals(expectedDecimal, converted.decimalValue());
  }

  @Test
  public void dateToJson() {
    GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.DATE, 10000);
    java.util.Date date = calendar.getTime();

    JsonNode converted = converter.fromConnectData(Date.SCHEMA, date);
    assertTrue(converted.isTextual());
    assertEquals(SimpleJsonConverter.ISO_DATE_FORMAT.format(date), converted.textValue());
  }

  @Test
  public void timeToJson() {
    GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.MILLISECOND, 14400000);
    java.util.Date date = calendar.getTime();

    JsonNode converted = converter.fromConnectData(Time.SCHEMA, date);
    assertTrue(converted.isTextual());
    assertEquals(SimpleJsonConverter.TIME_FORMAT.format(date), converted.textValue());
  }

  @Test
  public void timestampToJson() {
    GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.MILLISECOND, 2000000000);
    calendar.add(Calendar.MILLISECOND, 2000000000);
    java.util.Date date = calendar.getTime();

    JsonNode converted = converter.fromConnectData(Timestamp.SCHEMA, date);
    assertTrue(converted.isLong());
    assertEquals(4000000000L, converted.longValue());
  }


  @Test
  public void nullSchemaAndPrimitiveToJson() {
    // This still needs to do conversion of data, null schema means "anything goes"
    JsonNode converted = converter.fromConnectData(null, true);
    assertTrue(converted.booleanValue());
  }

  @Test
  public void nullSchemaAndArrayToJson() {
    // This still needs to do conversion of data, null schema means "anything goes". Make sure we mix and match
    // types to verify conversion still works.
    JsonNode converted = converter.fromConnectData(null, Arrays.asList(1, "string", true));
    assertEquals(JsonNodeFactory.instance.arrayNode().add(1).add("string").add(true), converted);
  }

  @Test
  public void nullSchemaAndMapToJson() {
    // This still needs to do conversion of data, null schema means "anything goes". Make sure we mix and match
    // types to verify conversion still works.
    Map<String, Object> input = new HashMap<>();
    input.put("key1", 12);
    input.put("key2", "string");
    input.put("key3", true);
    JsonNode converted = converter.fromConnectData(null, input);
    assertEquals(JsonNodeFactory.instance.objectNode().put("key1", 12).put("key2", "string").put("key3", true),
            converted);
  }

  @Test
  public void nullSchemaAndMapNonStringKeysToJson() {
    // This still needs to do conversion of data, null schema means "anything goes". Make sure we mix and match
    // types to verify conversion still works.
    Map<Object, Object> input = new HashMap<>();
    input.put("string", 12);
    input.put(52, "string");
    input.put(false, true);
    JsonNode converted = converter.fromConnectData(null, input);
    assertTrue(converted.isArray());
    ArrayNode payload = (ArrayNode) converted;
    assertEquals(3, payload.size());
    Set<JsonNode> payloadEntries = new HashSet<>();
    for (JsonNode elem : payload)
      payloadEntries.add(elem);
    assertEquals(new HashSet<>(Arrays.asList(JsonNodeFactory.instance.arrayNode().add("string").add(12),
            JsonNodeFactory.instance.arrayNode().add(52).add("string"),
            JsonNodeFactory.instance.arrayNode().add(false).add(true))),
            payloadEntries
    );
  }


  @Test(expected = DataException.class)
  public void mismatchSchemaJson() {
    // If we have mismatching schema info, we should properly convert to a DataException
    converter.fromConnectData(Schema.FLOAT64_SCHEMA, true);
  }


  @Test
  public void noSchemaToJson() {
    JsonNode converted = converter.fromConnectData(null, true);
    assertTrue(converted.isBoolean());
    assertTrue(converted.booleanValue());
  }

  @Test
  public void identicalSchemasShouldNotMismatch() {
    Schema schema1 = SchemaBuilder.struct().field("myString", Schema.STRING_SCHEMA).build();
    Schema schema2 = SchemaBuilder.struct().field("myString", Schema.STRING_SCHEMA).build();

    assertEquals(schema1, schema2);

    Struct struct1 = new Struct(schema1);
    struct1.put("myString", "testString");

    JsonNode converted = converter.fromConnectData(schema2, struct1);

    assertEquals("{\"myString\":\"testString\"}", converted.toString());
  }
}