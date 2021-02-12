/*
 * Copyright 2017 Datamountaineer.
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

package com.datamountaineer.streamreactor.common.converters;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;
 
/**
 * Pass-through converter for raw byte data.
 */
public class ByteArrayConverter implements Converter {
 
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }
 
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema != null && schema.type() != Schema.Type.BYTES && !schema.equals(Schema.OPTIONAL_BYTES_SCHEMA)) {
            throw new DataException("Invalid schema type for ByteArrayConverter: " + schema.type().toString());
        }

        if (value != null && !(value instanceof byte[])) {
            throw new DataException("ByteArrayConverter is not compatible with objects of type " + value.getClass());
        }
 
        return (byte[]) value;
    }
 
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, value);
    }
 
}