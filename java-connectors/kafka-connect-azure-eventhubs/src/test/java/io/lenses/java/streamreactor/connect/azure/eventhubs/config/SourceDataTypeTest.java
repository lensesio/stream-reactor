package io.lenses.java.streamreactor.connect.azure.eventhubs.config;

import static org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

class SourceDataTypeTest {

  @Test
  void fromNameShouldReturnEnum() {
    //given

    //when
    SourceDataType s = SourceDataType.fromName(SourceDataType.BYTES.name());

    //then
    assertEquals(SourceDataType.BYTES, s);
  }

  @Test
  void getDeserializerClassShouldReturnSpecifiedDeserializer() {
    //given

    //when
    Class<? extends Deserializer> deserializerClass = SourceDataType.BYTES.getDeserializerClass();

    //then
    assertEquals(ByteArrayDeserializer.class, deserializerClass);
  }

  @Test
  void getSchema() {
    //given

    //when
    Schema schema = SourceDataType.BYTES.getSchema();

    //then
    assertEquals(OPTIONAL_BYTES_SCHEMA, schema);
  }
}