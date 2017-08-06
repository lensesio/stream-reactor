package com.datamountaineer.kcql;


import java.util.ArrayList;
import java.util.List;

public class Field {
    private final String name;
    private final String alias;
    private final FieldType fieldType;
    private final List<String> parentFields;

    Field(String name, String alias, FieldType fieldType) {
        this(name, alias, fieldType, null);
    }

    Field(String name, FieldType fieldType, List<String> parents) {
        this(name, name, fieldType, parents);
    }

    Field(String name, String alias, FieldType fieldType, List<String> parents) {
        if (name == null || name.trim().length() == 0) {
            throw new IllegalArgumentException(String.format("field is not valid:<%s>", String.valueOf(name)));
        }
        if (alias == null || alias.trim().length() == 0) {
            throw new IllegalArgumentException(String.format("alias is not valid:<%s>", String.valueOf(alias)));
        }
        this.name = name;
        this.alias = alias;
        this.fieldType = fieldType;
        this.parentFields = parents;
    }

    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }

    public boolean hasParents() {
        return parentFields != null;
    }

    public List<String> getParentFields() {
        if (parentFields == null) return null;
        return new ArrayList<>(parentFields);
    }

    public String toString() {
        if (parentFields == null || parentFields.isEmpty()) return name;
        StringBuilder sb = new StringBuilder(parentFields.get(0));
        for (int i = 1; i < parentFields.size(); ++i) {
            sb.append(".");
            sb.append(parentFields.get(i));
        }
        sb.append(".");
        sb.append(name);
        return sb.toString();
    }

    public static Field from(String name, List<String> parents) {
        return from(name, null, parents);
    }

    public static Field from(String name, String alias, List<String> parents) {
        if (parents != null) {
            if (UNDERSCORE.equals(parents.get(0))) {
                switch (name.toLowerCase()) {

                    case TOPIC:
                        if (alias != null) {
                            return new Field(TOPIC, alias, FieldType.TOPIC);
                        }
                        return new Field(TOPIC, TOPIC, FieldType.TOPIC);

                    case OFFSET:
                        if (alias != null) {
                            return new Field(OFFSET, alias, FieldType.OFFSET);
                        }

                        return new Field(OFFSET, OFFSET, FieldType.OFFSET);

                    case TIMESTAMP:
                        if (alias != null) {
                            return new Field(TIMESTAMP, alias, FieldType.TIMESTAMP);
                        }
                        return new Field(TIMESTAMP, TIMESTAMP, FieldType.TIMESTAMP);

                    case PARTITION:
                        if (alias != null) {
                            return new Field(PARTITION, alias, FieldType.PARTITION);
                        }
                        return new Field(PARTITION, PARTITION, FieldType.PARTITION);

                    default:
                        if (parents.size() <= 1 || !"key".equals(parents.get(1).toLowerCase())) {
                            throw new IllegalArgumentException(String.format("Invalid syntax. '_' needs to be followed by: key,%s,%s,%s,%s", TOPIC, PARTITION, TIMESTAMP, OFFSET));
                        }

                        if (parents.size() <= 2) {
                            if (alias != null) {
                                if ("*".equals(name)) {
                                    throw new IllegalArgumentException("You can't alias '*'.");
                                }
                                return new Field(name, alias, FieldType.KEY, null);
                            }
                            return new Field(name, FieldType.KEY, null);
                        }

                        List<String> parentsCopy = new ArrayList<>();
                        for (int i = 2; i < parents.size(); ++i) {
                            parentsCopy.add(parents.get(i));
                        }
                        if (alias != null) {
                            return new Field(name, alias, FieldType.KEY, parentsCopy);
                        }
                        return new Field(name, FieldType.KEY, parentsCopy);

                }
            }
        }
        List<String> parentsCopy = null;
        if (parents != null) {
            parentsCopy = new ArrayList<>(parents);
        }
        if (alias != null) {
            return new Field(name, alias, FieldType.VALUE, parentsCopy);
        }
        return new Field(name, FieldType.VALUE, parentsCopy);
    }

    private static final String UNDERSCORE = "_";
    private static final String OFFSET = "offset";
    private static final String TOPIC = "topic";
    private static final String PARTITION = "partition";
    private static final String TIMESTAMP = "timestamp";

    public FieldType getFieldType() {
        return fieldType;
    }
}
