package com.datamountaineer.streamreactor.common.converters;

import com.google.common.io.BaseEncoding;
import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.FieldMaskUtil;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

public class ParserImpl {
    private final TypeRegistry registry;
    private final JsonFormat.TypeRegistry oldRegistry;
    private final boolean ignoringUnknownFields;
    private final int recursionLimit;
    private int currentDepth;

    public ParserImpl() {
        this(TypeRegistry.getEmptyTypeRegistry(),
                JsonFormat.TypeRegistry.getEmptyTypeRegistry(),
                false,
                100);
    }

    public ParserImpl(
            TypeRegistry registry,
            JsonFormat.TypeRegistry oldRegistry,
            boolean ignoreUnknownFields,
            int recursionLimit) {
        this.registry = registry;
        this.oldRegistry = oldRegistry;
        this.ignoringUnknownFields = ignoreUnknownFields;
        this.recursionLimit = recursionLimit;
        this.currentDepth = 0;
    }

    void merge(Reader json, Message.Builder builder) throws IOException {
        try {
            JsonReader reader = new JsonReader(json);
            reader.setLenient(false);
            merge(JsonParser.parseReader(reader), builder);
        } catch (InvalidProtocolBufferException e) {
            throw e;
        } catch (JsonIOException e) {
            // Unwrap IOException.
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new InvalidProtocolBufferException(e.getMessage());
            }
        } catch (Exception e) {
            // We convert all exceptions from JSON parsing to our own exceptions.
            throw new InvalidProtocolBufferException(e.getMessage());
        }
    }

    public void merge(String json, Message.Builder builder) throws InvalidProtocolBufferException {
        try {
            JsonReader reader = new JsonReader(new StringReader(json));
            reader.setLenient(false);
            JsonElement jsonelement = JsonParser.parseReader(reader);
            System.out.println("JsonElement: "+ jsonelement);
            merge(jsonelement, builder);
        } catch (InvalidProtocolBufferException e) {
            throw e;
        } catch (Exception e) {
            // We convert all exceptions from JSON parsing to our own exceptions.
            throw new InvalidProtocolBufferException(e.getMessage());
        }
    }

    private interface WellKnownTypeParser {
        void merge(ParserImpl parser, JsonElement json, Message.Builder builder)
                throws InvalidProtocolBufferException;
    }

    private static final Map<String, WellKnownTypeParser> wellKnownTypeParsers =
            buildWellKnownTypeParsers();

    private static Map<String, WellKnownTypeParser> buildWellKnownTypeParsers() {
        Map<String, WellKnownTypeParser> parsers = new HashMap<String, WellKnownTypeParser>();
        // Special-case Any.
        parsers.put(
                Any.getDescriptor().getFullName(),
                new WellKnownTypeParser() {
                    @Override
                    public void merge(ParserImpl parser, JsonElement json, Message.Builder builder)
                            throws InvalidProtocolBufferException {
                        parser.mergeAny(json, builder);
                    }
                });
        // Special-case wrapper types.
        WellKnownTypeParser wrappersPrinter =
                new WellKnownTypeParser() {
                    @Override
                    public void merge(ParserImpl parser, JsonElement json, Message.Builder builder)
                            throws InvalidProtocolBufferException {
                        parser.mergeWrapper(json, builder);
                    }
                };
        parsers.put(BoolValue.getDescriptor().getFullName(), wrappersPrinter);
        parsers.put(Int32Value.getDescriptor().getFullName(), wrappersPrinter);
        parsers.put(UInt32Value.getDescriptor().getFullName(), wrappersPrinter);
        parsers.put(Int64Value.getDescriptor().getFullName(), wrappersPrinter);
        parsers.put(UInt64Value.getDescriptor().getFullName(), wrappersPrinter);
        parsers.put(StringValue.getDescriptor().getFullName(), wrappersPrinter);
        parsers.put(BytesValue.getDescriptor().getFullName(), wrappersPrinter);
        parsers.put(FloatValue.getDescriptor().getFullName(), wrappersPrinter);
        parsers.put(DoubleValue.getDescriptor().getFullName(), wrappersPrinter);
        // Special-case Timestamp.
        parsers.put(
                Timestamp.getDescriptor().getFullName(),
                new WellKnownTypeParser() {
                    @Override
                    public void merge(ParserImpl parser, JsonElement json, Message.Builder builder)
                            throws InvalidProtocolBufferException {
                        parser.mergeTimestamp(json, builder);
                    }
                });
        // Special-case Duration.
        parsers.put(
                Duration.getDescriptor().getFullName(),
                new WellKnownTypeParser() {
                    @Override
                    public void merge(ParserImpl parser, JsonElement json, Message.Builder builder)
                            throws InvalidProtocolBufferException {
                        parser.mergeDuration(json, builder);
                    }
                });
        // Special-case FieldMask.
        parsers.put(
                FieldMask.getDescriptor().getFullName(),
                new WellKnownTypeParser() {
                    @Override
                    public void merge(ParserImpl parser, JsonElement json, Message.Builder builder)
                            throws InvalidProtocolBufferException {
                        parser.mergeFieldMask(json, builder);
                    }
                });
        // Special-case Struct.
        parsers.put(
                Struct.getDescriptor().getFullName(),
                new WellKnownTypeParser() {
                    @Override
                    public void merge(ParserImpl parser, JsonElement json, Message.Builder builder)
                            throws InvalidProtocolBufferException {
                        parser.mergeStruct(json, builder);
                    }
                });
        // Special-case ListValue.
        parsers.put(
                ListValue.getDescriptor().getFullName(),
                new WellKnownTypeParser() {
                    @Override
                    public void merge(ParserImpl parser, JsonElement json, Message.Builder builder)
                            throws InvalidProtocolBufferException {
                        parser.mergeListValue(json, builder);
                    }
                });
        // Special-case Value.
        parsers.put(
                Value.getDescriptor().getFullName(),
                new WellKnownTypeParser() {
                    @Override
                    public void merge(ParserImpl parser, JsonElement json, Message.Builder builder)
                            throws InvalidProtocolBufferException {
                        parser.mergeValue(json, builder);
                    }
                });
        return parsers;
    }

    private void merge(JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        WellKnownTypeParser specialParser =
                wellKnownTypeParsers.get(builder.getDescriptorForType().getFullName());
        if (specialParser != null) {
            specialParser.merge(this, json, builder);
            return;
        }
        mergeMessage(json, builder, false);
    }

    // Maps from camel-case field names to FieldDescriptor.
    private final Map<Descriptor, Map<String, FieldDescriptor>> fieldNameMaps =
            new HashMap<Descriptor, Map<String, FieldDescriptor>>();

    private Map<String, FieldDescriptor> getFieldNameMap(Descriptor descriptor) {
        if (!fieldNameMaps.containsKey(descriptor)) {
            Map<String, FieldDescriptor> fieldNameMap = new HashMap<String, FieldDescriptor>();
            for (FieldDescriptor field : descriptor.getFields()) {
                fieldNameMap.put(field.getName(), field);
                fieldNameMap.put(field.getJsonName(), field);
            }
            fieldNameMaps.put(descriptor, fieldNameMap);
            return fieldNameMap;
        }
        return fieldNameMaps.get(descriptor);
    }

    private void mergeMessage(JsonElement json, Message.Builder builder, boolean skipTypeUrl)
            throws InvalidProtocolBufferException {
        if (!(json instanceof JsonObject)) {
            throw new InvalidProtocolBufferException("Expect message object but got: " + json);
        }
        JsonObject object = (JsonObject) json;
        Map<String, FieldDescriptor> fieldNameMap = getFieldNameMap(builder.getDescriptorForType());
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            if (skipTypeUrl && entry.getKey().equals("@type")) {
                continue;
            }
            FieldDescriptor field = fieldNameMap.get(entry.getKey());
            if (field == null) {
                if (ignoringUnknownFields) {
                    continue;
                }
                throw new InvalidProtocolBufferException(
                        "Cannot find field: "
                                + entry.getKey()
                                + " in message "
                                + builder.getDescriptorForType().getFullName());
            }
            mergeField(field, entry.getValue(), builder);
        }
    }

    private void mergeAny(JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        Descriptor descriptor = builder.getDescriptorForType();
        FieldDescriptor typeUrlField = descriptor.findFieldByName("type_url");
        FieldDescriptor valueField = descriptor.findFieldByName("value");
        // Validates type of the message. Note that we can't just cast the message
        // to com.google.protobuf.Any because it might be a DynamicMessage.
        if (typeUrlField == null
                || valueField == null
                || typeUrlField.getType() != FieldDescriptor.Type.STRING
                || valueField.getType() != FieldDescriptor.Type.BYTES) {
            throw new InvalidProtocolBufferException("Invalid Any type.");
        }

        if (!(json instanceof JsonObject)) {
            throw new InvalidProtocolBufferException("Expect message object but got: " + json);
        }
        JsonObject object = (JsonObject) json;
        if (object.entrySet().isEmpty()) {
            return; // builder never modified, so it will end up building the default instance of Any
        }
        JsonElement typeUrlElement = object.get("@type");
        if (typeUrlElement == null) {
            throw new InvalidProtocolBufferException("Missing type url when parsing: " + json);
        }
        String typeUrl = typeUrlElement.getAsString();
        Descriptor contentType = registry.getDescriptorForTypeUrl(typeUrl);
        if (contentType == null) {
            contentType = oldRegistry.find(getTypeName(typeUrl));
            if (contentType == null) {
                throw new InvalidProtocolBufferException("Cannot resolve type: " + typeUrl);
            }
        }
        builder.setField(typeUrlField, typeUrl);
        Message.Builder contentBuilder =
                DynamicMessage.getDefaultInstance(contentType).newBuilderForType();
        WellKnownTypeParser specialParser = wellKnownTypeParsers.get(contentType.getFullName());
        if (specialParser != null) {
            JsonElement value = object.get("value");
            if (value != null) {
                specialParser.merge(this, value, contentBuilder);
            }
        } else {
            mergeMessage(json, contentBuilder, true);
        }
        builder.setField(valueField, contentBuilder.build().toByteString());
    }



    private void mergeFieldMask(JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        FieldMask value = FieldMaskUtil.fromJsonString(json.getAsString());
        builder.mergeFrom(value.toByteString());
    }

    private void mergeTimestamp(JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        try {
            Timestamp value;
            if (json.isJsonObject()) {
                long seconds = json.getAsJsonObject().get("seconds").getAsLong();
                int nanos = json.getAsJsonObject().get("nanos").getAsInt();
                value = Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
            } else {
                value = Timestamps.parse(json.getAsString());
            }
            builder.mergeFrom(value.toByteString());
        } catch (Exception e) {
            e.printStackTrace();
            throw new InvalidProtocolBufferException("Failed to parse timestamp: " + json);
        }
    }

    private void mergeDuration(JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        try {
            Duration value = Durations.parse(json.getAsString());
            builder.mergeFrom(value.toByteString());
        } catch (ParseException e) {
            throw new InvalidProtocolBufferException("Failed to parse duration: " + json);
        }
    }

    private void mergeStruct(JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        Descriptor descriptor = builder.getDescriptorForType();
        FieldDescriptor field = descriptor.findFieldByName("fields");
        if (field == null) {
            throw new InvalidProtocolBufferException("Invalid Struct type.");
        }
        mergeMapField(field, json, builder);
    }

    private void mergeListValue(JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        Descriptor descriptor = builder.getDescriptorForType();
        FieldDescriptor field = descriptor.findFieldByName("values");
        if (field == null) {
            throw new InvalidProtocolBufferException("Invalid ListValue type.");
        }
        mergeRepeatedField(field, json, builder);
    }

    private void mergeValue(JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        Descriptor type = builder.getDescriptorForType();
        if (json instanceof JsonPrimitive) {
            JsonPrimitive primitive = (JsonPrimitive) json;
            if (primitive.isBoolean()) {
                builder.setField(type.findFieldByName("bool_value"), primitive.getAsBoolean());
            } else if (primitive.isNumber()) {
                builder.setField(type.findFieldByName("number_value"), primitive.getAsDouble());
            } else {
                builder.setField(type.findFieldByName("string_value"), primitive.getAsString());
            }
        } else if (json instanceof JsonObject) {
            FieldDescriptor field = type.findFieldByName("struct_value");
            Message.Builder structBuilder = builder.newBuilderForField(field);
            merge(json, structBuilder);
            builder.setField(field, structBuilder.build());
        } else if (json instanceof JsonArray) {
            FieldDescriptor field = type.findFieldByName("list_value");
            Message.Builder listBuilder = builder.newBuilderForField(field);
            merge(json, listBuilder);
            builder.setField(field, listBuilder.build());
        } else if (json instanceof JsonNull) {
            builder.setField(
                    type.findFieldByName("null_value"), NullValue.NULL_VALUE.getValueDescriptor());
        } else {
            throw new IllegalStateException("Unexpected json data: " + json);
        }
    }

    private void mergeWrapper(JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        Descriptor type = builder.getDescriptorForType();
        FieldDescriptor field = type.findFieldByName("value");
        if (field == null) {
            throw new InvalidProtocolBufferException("Invalid wrapper type: " + type.getFullName());
        }
        builder.setField(field, parseFieldValue(field, json, builder));
    }

    private void mergeField(FieldDescriptor field, JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        if (field.isRepeated()) {
            if (builder.getRepeatedFieldCount(field) > 0) {
                throw new InvalidProtocolBufferException(
                        "Field " + field.getFullName() + " has already been set.");
            }
        } else {
            if (builder.hasField(field)) {
                throw new InvalidProtocolBufferException(
                        "Field " + field.getFullName() + " has already been set.");
            }
        }
        if (field.isRepeated() && json instanceof JsonNull) {
            // We allow "null" as value for all field types and treat it as if the
            // field is not present.
            return;
        }
        if (field.isMapField()) {
            mergeMapField(field, json, builder);
        } else if (field.isRepeated()) {
            mergeRepeatedField(field, json, builder);
        } else if (field.getContainingOneof() != null) {
            mergeOneofField(field, json, builder);
        } else {
            Object value = parseFieldValue(field, json, builder);
            if (value != null) {
                // A field interpreted as "null" is means it's treated as absent.
                builder.setField(field, value);
            }
        }
    }

    private void mergeMapField(FieldDescriptor field, JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        if (!(json instanceof JsonObject)) {
            throw new InvalidProtocolBufferException("Expect a map object but found: " + json);
        }
        Descriptor type = field.getMessageType();
        FieldDescriptor keyField = type.findFieldByName("key");
        FieldDescriptor valueField = type.findFieldByName("value");
        if (keyField == null || valueField == null) {
            throw new InvalidProtocolBufferException("Invalid map field: " + field.getFullName());
        }
        JsonObject object = (JsonObject) json;
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            Message.Builder entryBuilder = builder.newBuilderForField(field);
            Object key = parseFieldValue(keyField, new JsonPrimitive(entry.getKey()), entryBuilder);
            Object value = parseFieldValue(valueField, entry.getValue(), entryBuilder);
            if (value == null) {
                if (ignoringUnknownFields && valueField.getType() == FieldDescriptor.Type.ENUM) {
                    continue;
                } else {
                    throw new InvalidProtocolBufferException("Map value cannot be null.");
                }
            }
            entryBuilder.setField(keyField, key);
            entryBuilder.setField(valueField, value);
            builder.addRepeatedField(field, entryBuilder.build());
        }
    }

    private void mergeOneofField(FieldDescriptor field, JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        Object value = parseFieldValue(field, json, builder);
        if (value == null) {
            // A field interpreted as "null" is means it's treated as absent.
            return;
        }
        if (builder.getOneofFieldDescriptor(field.getContainingOneof()) != null) {
            throw new InvalidProtocolBufferException(
                    "Cannot set field "
                            + field.getFullName()
                            + " because another field "
                            + builder.getOneofFieldDescriptor(field.getContainingOneof()).getFullName()
                            + " belonging to the same oneof has already been set ");
        }
        builder.setField(field, value);
    }

    private void mergeRepeatedField(
            FieldDescriptor field, JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        if (!(json instanceof JsonArray)) {
            throw new InvalidProtocolBufferException("Expect an array but found: " + json);
        }
        JsonArray array = (JsonArray) json;
        for (int i = 0; i < array.size(); ++i) {
            Object value = parseFieldValue(field, array.get(i), builder);
            if (value == null) {
                if (ignoringUnknownFields && field.getType() == FieldDescriptor.Type.ENUM) {
                    continue;
                } else {
                    throw new InvalidProtocolBufferException(
                            "Repeated field elements cannot be null in field: " + field.getFullName());
                }
            }
            builder.addRepeatedField(field, value);
        }
    }

    private int parseInt32(JsonElement json) throws InvalidProtocolBufferException {
        try {
            return Integer.parseInt(json.getAsString());
        } catch (Exception e) {
            // Fall through.
        }
        // JSON doesn't distinguish between integer values and floating point values so "1" and
        // "1.000" are treated as equal in JSON. For this reason we accept floating point values for
        // integer fields as well as long as it actually is an integer (i.e., round(value) == value).
        try {
            BigDecimal value = new BigDecimal(json.getAsString());
            return value.intValueExact();
        } catch (Exception e) {
            throw new InvalidProtocolBufferException("Not an int32 value: " + json);
        }
    }

    private long parseInt64(JsonElement json) throws InvalidProtocolBufferException {
        try {
            return Long.parseLong(json.getAsString());
        } catch (Exception e) {
            // Fall through.
        }
        // JSON doesn't distinguish between integer values and floating point values so "1" and
        // "1.000" are treated as equal in JSON. For this reason we accept floating point values for
        // integer fields as well as long as it actually is an integer (i.e., round(value) == value).
        try {
            BigDecimal value = new BigDecimal(json.getAsString());
            return value.longValueExact();
        } catch (Exception e) {
            throw new InvalidProtocolBufferException("Not an int64 value: " + json);
        }
    }

    private int parseUint32(JsonElement json) throws InvalidProtocolBufferException {
        try {
            long result = Long.parseLong(json.getAsString());
            if (result < 0 || result > 0xFFFFFFFFL) {
                throw new InvalidProtocolBufferException("Out of range uint32 value: " + json);
            }
            return (int) result;
        } catch (InvalidProtocolBufferException e) {
            throw e;
        } catch (Exception e) {
            // Fall through.
        }
        // JSON doesn't distinguish between integer values and floating point values so "1" and
        // "1.000" are treated as equal in JSON. For this reason we accept floating point values for
        // integer fields as well as long as it actually is an integer (i.e., round(value) == value).
        try {
            BigDecimal decimalValue = new BigDecimal(json.getAsString());
            BigInteger value = decimalValue.toBigIntegerExact();
            if (value.signum() < 0 || value.compareTo(new BigInteger("FFFFFFFF", 16)) > 0) {
                throw new InvalidProtocolBufferException("Out of range uint32 value: " + json);
            }
            return value.intValue();
        } catch (InvalidProtocolBufferException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidProtocolBufferException("Not an uint32 value: " + json);
        }
    }

    private static final BigInteger MAX_UINT64 = new BigInteger("FFFFFFFFFFFFFFFF", 16);

    private long parseUint64(JsonElement json) throws InvalidProtocolBufferException {
        try {
            BigDecimal decimalValue = new BigDecimal(json.getAsString());
            BigInteger value = decimalValue.toBigIntegerExact();
            if (value.compareTo(BigInteger.ZERO) < 0 || value.compareTo(MAX_UINT64) > 0) {
                throw new InvalidProtocolBufferException("Out of range uint64 value: " + json);
            }
            return value.longValue();
        } catch (InvalidProtocolBufferException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidProtocolBufferException("Not an uint64 value: " + json);
        }
    }

    private boolean parseBool(JsonElement json) throws InvalidProtocolBufferException {
        if (json.getAsString().equals("true")) {
            return true;
        }
        if (json.getAsString().equals("false")) {
            return false;
        }
        throw new InvalidProtocolBufferException("Invalid bool value: " + json);
    }

    private static final double EPSILON = 1e-6;

    private float parseFloat(JsonElement json) throws InvalidProtocolBufferException {
        if (json.getAsString().equals("NaN")) {
            return Float.NaN;
        } else if (json.getAsString().equals("Infinity")) {
            return Float.POSITIVE_INFINITY;
        } else if (json.getAsString().equals("-Infinity")) {
            return Float.NEGATIVE_INFINITY;
        }
        try {
            // We don't use Float.parseFloat() here because that function simply
            // accepts all double values. Here we parse the value into a Double
            // and do explicit range check on it.
            double value = Double.parseDouble(json.getAsString());
            // When a float value is printed, the printed value might be a little
            // larger or smaller due to precision loss. Here we need to add a bit
            // of tolerance when checking whether the float value is in range.
            if (value > Float.MAX_VALUE * (1.0 + EPSILON)
                    || value < -Float.MAX_VALUE * (1.0 + EPSILON)) {
                throw new InvalidProtocolBufferException("Out of range float value: " + json);
            }
            return (float) value;
        } catch (InvalidProtocolBufferException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidProtocolBufferException("Not a float value: " + json);
        }
    }

    private static final BigDecimal MORE_THAN_ONE = new BigDecimal(String.valueOf(1.0 + EPSILON));
    // When a float value is printed, the printed value might be a little
    // larger or smaller due to precision loss. Here we need to add a bit
    // of tolerance when checking whether the float value is in range.
    private static final BigDecimal MAX_DOUBLE =
            new BigDecimal(String.valueOf(Double.MAX_VALUE)).multiply(MORE_THAN_ONE);
    private static final BigDecimal MIN_DOUBLE =
            new BigDecimal(String.valueOf(-Double.MAX_VALUE)).multiply(MORE_THAN_ONE);

    private double parseDouble(JsonElement json) throws InvalidProtocolBufferException {
        if (json.getAsString().equals("NaN")) {
            return Double.NaN;
        } else if (json.getAsString().equals("Infinity")) {
            return Double.POSITIVE_INFINITY;
        } else if (json.getAsString().equals("-Infinity")) {
            return Double.NEGATIVE_INFINITY;
        }
        try {
            // We don't use Double.parseDouble() here because that function simply
            // accepts all values. Here we parse the value into a BigDecimal and do
            // explicit range check on it.
            BigDecimal value = new BigDecimal(json.getAsString());
            if (value.compareTo(MAX_DOUBLE) > 0 || value.compareTo(MIN_DOUBLE) < 0) {
                throw new InvalidProtocolBufferException("Out of range double value: " + json);
            }
            return value.doubleValue();
        } catch (InvalidProtocolBufferException e) {
            throw e;
        } catch (Exception e) {
            throw new InvalidProtocolBufferException("Not an double value: " + json);
        }
    }

    private String parseString(JsonElement json) {
        return json.getAsString();
    }

    private ByteString parseBytes(JsonElement json) throws InvalidProtocolBufferException {
        try {
            return ByteString.copyFrom(BaseEncoding.base64().decode(json.getAsString()));
        } catch (IllegalArgumentException e) {
            return ByteString.copyFrom(BaseEncoding.base64Url().decode(json.getAsString()));
        }
    }

    private Descriptors.EnumValueDescriptor parseEnum(Descriptors.EnumDescriptor enumDescriptor, JsonElement json)
            throws InvalidProtocolBufferException {
        String value = json.getAsString();
        Descriptors.EnumValueDescriptor result = enumDescriptor.findValueByName(value);
        if (result == null) {
            // Try to interpret the value as a number.
            try {
                int numericValue = parseInt32(json);
                if (enumDescriptor.getFile().getSyntax() == Descriptors.FileDescriptor.Syntax.PROTO3) {
                    result = enumDescriptor.findValueByNumberCreatingIfUnknown(numericValue);
                } else {
                    result = enumDescriptor.findValueByNumber(numericValue);
                }
            } catch (InvalidProtocolBufferException e) {
                // Fall through. This exception is about invalid int32 value we get from parseInt32() but
                // that's not the exception we want the user to see. Since result == null, we will throw
                // an exception later.
            }

            if (result == null && !ignoringUnknownFields) {
                throw new InvalidProtocolBufferException(
                        "Invalid enum value: " + value + " for enum type: " + enumDescriptor.getFullName());
            }
        }
        return result;
    }

    private Object parseFieldValue(FieldDescriptor field, JsonElement json, Message.Builder builder)
            throws InvalidProtocolBufferException {
        if (json instanceof JsonNull) {
            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    && field.getMessageType().getFullName().equals(Value.getDescriptor().getFullName())) {
                // For every other type, "null" means absence, but for the special
                // Value message, it means the "null_value" field has been set.
                Value value = Value.newBuilder().setNullValueValue(0).build();
                return builder.newBuilderForField(field).mergeFrom(value.toByteString()).build();
            } else if (field.getJavaType() == FieldDescriptor.JavaType.ENUM
                    && field.getEnumType().getFullName().equals(NullValue.getDescriptor().getFullName())) {
                // If the type of the field is a NullValue, then the value should be explicitly set.
                return field.getEnumType().findValueByNumber(0);
            }
            return null;
        } else if (json instanceof JsonObject) {
            if (field.getType() != FieldDescriptor.Type.MESSAGE
                    && field.getType() != FieldDescriptor.Type.GROUP) {
                // If the field type is primitive, but the json type is JsonObject rather than
                // JsonElement, throw a type mismatch error.
                throw new InvalidProtocolBufferException(
                        String.format("Invalid value: %s for expected type: %s", json, field.getType()));
            }
        }
        switch (field.getType()) {
            case INT32:
            case SINT32:
            case SFIXED32:
                return parseInt32(json);

            case INT64:
            case SINT64:
            case SFIXED64:
                return parseInt64(json);

            case BOOL:
                return parseBool(json);

            case FLOAT:
                return parseFloat(json);

            case DOUBLE:
                return parseDouble(json);

            case UINT32:
            case FIXED32:
                return parseUint32(json);

            case UINT64:
            case FIXED64:
                return parseUint64(json);

            case STRING:
                return parseString(json);

            case BYTES:
                return parseBytes(json);

            case ENUM:
                return parseEnum(field.getEnumType(), json);

            case MESSAGE:
            case GROUP:
                if (currentDepth >= recursionLimit) {
                    throw new InvalidProtocolBufferException("Hit recursion limit.");
                }
                ++currentDepth;
                Message.Builder subBuilder = builder.newBuilderForField(field);
                merge(json, subBuilder);
                --currentDepth;
                return subBuilder.build();

            default:
                throw new InvalidProtocolBufferException("Invalid field type: " + field.getType());
        }
    }

    private static String getTypeName(String typeUrl) throws InvalidProtocolBufferException {
        String[] parts = typeUrl.split("/");
        if (parts.length == 1) {
            throw new InvalidProtocolBufferException("Invalid type url found: " + typeUrl);
        }
        return parts[parts.length - 1];
    }
}