package com.datamountaineer.kcql;

import com.datamountaineer.kcql.antlr4.ConnectorLexer;
import com.datamountaineer.kcql.antlr4.ConnectorParser;
import com.datamountaineer.kcql.antlr4.ConnectorParserBaseListener;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.OrderedHashSet;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;

/**
 * Parsing support for Kafka Connect Query Language.
 */
public class Kcql {

    public final static String TIMESTAMP = "sys_time()";
    public final static int DEFAULT_BATCH_SIZE = 3000;
    /**
     * Returns true if all payload fields should be included; false - otherwise
     */
    private boolean includeAllFields;
    private boolean autoCreate;
    private boolean autoEvolve;
    private boolean enableCapitalize;
    private WriteModeEnum writeMode;
    private String source;
    private String target;
    private Map<String, Field> fields = new HashMap<>();
    private Set<String> ignoredFields = new HashSet<>();
    private Set<String> primaryKeys = new OrderedHashSet<>();
    private List<String> partitionBy = new ArrayList<>();
    private int retries = 1;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private Bucketing bucketing;
    private String timestamp;
    private String storedAs;
    private Map<String, String> storedAsParameters = new HashMap<>();
    private String consumerGroup;
    private List<PartitionOffset> partitions = null;
    private Integer sampleCount;
    private Integer sampleRate;
    private FormatType formatType = null;
    private boolean initialize;
    private Integer projectTo;
    private List<Tag> tags;

    private void addIgnoredField(final String ignoredField) {
        if (ignoredField == null || ignoredField.trim().length() == 0) {
            throw new IllegalArgumentException("Invalid ignoredField");
        }
        ignoredFields.add(ignoredField);
    }

    private void addFieldAlias(final Field fieldAlias) {
        if (fieldAlias == null) {
            throw new IllegalArgumentException("Illegal fieldAlias.");
        }
        fields.put(fieldAlias.getName(), fieldAlias);
    }

    private void addPrimaryKey(final String primaryKey) {
        if (primaryKey == null || primaryKey.trim().length() == 0) {
            throw new IllegalArgumentException("Invalid primaryKey.");
        }
        primaryKeys.add(primaryKey);
    }

    private void addPartitionByField(final String field) {
        if (field == null || field.trim().length() == 0) {
            throw new IllegalArgumentException("Invalid partition by field");
        }
        for (final String f : partitionBy) {
            if (f.compareToIgnoreCase(field.trim()) == 0) {
                throw new IllegalArgumentException(String.format("The field %s appears twice", field));
            }
        }
        partitionBy.add(field.trim());
    }

    public String getSource() {
        return source;
    }

    private void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    private void setTarget(String target) {
        this.target = target;
    }

    public List<Field> getFieldAlias() {
        return new ArrayList<>(fields.values());
    }

    public Set<String> getIgnoredField() {
        return new HashSet<>(ignoredFields);
    }

    public boolean isIncludeAllFields() {
        return includeAllFields;
    }

    private void setIncludeAllFields() {
        this.includeAllFields = true;
    }

    public WriteModeEnum getWriteMode() {
        return writeMode;
    }

    private void setWriteMode(WriteModeEnum writeMode) {
        this.writeMode = writeMode;
    }

    public Iterator<String> getPrimaryKeys() {
        return primaryKeys.iterator();
    }

    public Bucketing getBucketing() {
        return bucketing;
    }

    private void setBucketing(final Bucketing bucketing) {
        this.bucketing = bucketing;
    }

    public String getTimestamp() {
        return this.timestamp;
    }

    private void setTimestamp(final String value) {
        this.timestamp = value;
    }

    public String getStoredAs() {
        return storedAs;
    }

    private void setStoredAs(String format) {
        this.storedAs = format;
    }

    public Map<String, String> getStoredAsParameters() {
        return storedAsParameters;
    }

    private void setStoredAsParameters(Map<String, String> storedAsParameters) {
        this.storedAsParameters = storedAsParameters;
    }

    private void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public List<PartitionOffset> getPartitonOffset() {
        return partitions;
    }

    public Integer getSampleCount() {
        return sampleCount;
    }

    private void setSampleCount(Integer sampleCount) {
        this.sampleCount = sampleCount;
    }

    public Integer getSampleRate() {
        return sampleRate;
    }

    private void setSampleRate(Integer sampleRate) {
        this.sampleRate = sampleRate;
    }

    public FormatType getFormatType() {
        return formatType;
    }

    private void setFormatType(FormatType type) {
        formatType = type;
    }

    public Integer getProjectTo() {
        return projectTo;
    }

    private void setProjectTo(Integer version) {
        this.projectTo = version;
    }

    public boolean isAutoCreate() {
        return autoCreate;
    }

    public void setAutoCreate(boolean autoCreate) {
        this.autoCreate = autoCreate;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public boolean isAutoEvolve() {
        return autoEvolve;
    }

    public void setAutoEvolve(boolean autoEvolve) {
        this.autoEvolve = autoEvolve;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isEnableCapitalize() {
        return enableCapitalize;
    }

    public void setEnableCapitalize(boolean enableCapitalize) {
        this.enableCapitalize = enableCapitalize;
    }

    public void setEnableInitialize(boolean enableInitialize) {
        this.initialize = enableInitialize;
    }

    public boolean isInitialize() {
        return initialize;
    }

    public Iterator<String> getPartitionBy() {
        return partitionBy.iterator();
    }

    public Iterator<Tag> getTags() {
        if (tags == null) {
            return new Iterator<Tag>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Tag next() {
                    return null;
                }
            };
        }
        return tags.iterator();
    }

    public static Kcql parse(final String syntax) {
        final ConnectorLexer lexer = new ConnectorLexer(new ANTLRInputStream(syntax));
        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final ConnectorParser parser = new ConnectorParser(tokens);
        final ArrayList<String> bucketNames = new ArrayList<>();
        final ArrayList<String> nestedFieldsBuffer = new ArrayList<>();
        final Integer[] bucketsNumber = {null};
        final Kcql kcql = new Kcql();
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer,
                                    Object offendingSymbol,
                                    int line,
                                    int charPositionInLine,
                                    String msg,
                                    RecognitionException e) {
                throw new IllegalStateException("failed to parse at line " + line + " due to " + msg, e);
            }
        });

        final String[] storedAsParameter = {null};

        parser.addParseListener(new ConnectorParserBaseListener() {

            @Override
            public void exitColumn(ConnectorParser.ColumnContext ctx) {
                for (TerminalNode tn : ctx.FIELD()) {
                    nestedFieldsBuffer.add(tn.getText());
                }
                if (ctx.ASTERISK() != null) {
                    nestedFieldsBuffer.add("*");
                }
            }

            @Override
            public void enterColumn_name(ConnectorParser.Column_nameContext ctx) {
                nestedFieldsBuffer.clear();
            }

            @Override
            public void exitColumn_name(ConnectorParser.Column_nameContext ctx) {
                super.exitColumn_name(ctx);
                if (ctx.ASTERISK() != null) {
                    kcql.setIncludeAllFields();
                    return;
                }

                List<String> parentFields = null;
                String name = nestedFieldsBuffer.get(nestedFieldsBuffer.size() - 1);
                nestedFieldsBuffer.remove(nestedFieldsBuffer.size() - 1);

                if (nestedFieldsBuffer.size() > 0) {
                    parentFields = nestedFieldsBuffer;
                }

                Field field;
                if (ctx.column_name_alias() != null) {
                    field = Field.from(name, ctx.column_name_alias().getText(), parentFields);
                } else {
                    field = Field.from(name, parentFields);
                }

                if ("*".equals(field.getName()) && !field.hasParents() && field.getFieldType() != FieldType.KEY) {
                    kcql.setIncludeAllFields();
                } else {
                    kcql.addFieldAlias(field);
                }
            }


            @Override
            public void exitPartition_name(ConnectorParser.Partition_nameContext ctx) {
                kcql.addPartitionByField(ctx.getText());
            }

            @Override
            public void exitDistribute_name(ConnectorParser.Distribute_nameContext ctx) {
                bucketNames.add(ctx.getText());
            }

            @Override
            public void exitTable_name(ConnectorParser.Table_nameContext ctx) {
                kcql.setTarget(escape(ctx.getText()));
            }

            @Override
            public void exitIgnored_name(ConnectorParser.Ignored_nameContext ctx) {
                kcql.addIgnoredField(ctx.getText());
            }

            @Override
            public void exitTopic_name(ConnectorParser.Topic_nameContext ctx) {
                kcql.setSource(escape(ctx.getText()));
            }

            @Override
            public void exitUpsert_into(ConnectorParser.Upsert_intoContext ctx) {
                kcql.setWriteMode(WriteModeEnum.UPSERT);
            }

            @Override
            public void exitInsert_into(ConnectorParser.Insert_intoContext ctx) {
                kcql.setWriteMode(WriteModeEnum.INSERT);
            }

            @Override
            public void exitAutocreate(ConnectorParser.AutocreateContext ctx) {
                kcql.setAutoCreate(true);
            }

            @Override
            public void exitPk_name(ConnectorParser.Pk_nameContext ctx) {
                kcql.addPrimaryKey(ctx.getText());
            }

            @Override
            public void exitAutoevolve(ConnectorParser.AutoevolveContext ctx) {
                kcql.setAutoEvolve(true);
            }

            @Override
            public void exitStoreas_type(ConnectorParser.Storeas_typeContext ctx) {
                kcql.setStoredAs(ctx.getText());
            }

            @Override
            public void exitStoreas_parameter(ConnectorParser.Storeas_parameterContext ctx) {
                String value = ctx.getText();
                for (String key : kcql.getStoredAsParameters().keySet()) {
                    if (key.compareToIgnoreCase(value) == 0) {
                        throw new IllegalArgumentException(value + " is a duplicated entry in the storeAs parameters list");
                    }
                }
                storedAsParameter[0] = value;
            }

            @Override
            public void exitStoreas_value(ConnectorParser.Storeas_valueContext ctx) {
                kcql.getStoredAsParameters().put(storedAsParameter[0], ctx.getText());
            }

            @Override
            public void exitCapitalize(ConnectorParser.CapitalizeContext ctx) {
                kcql.setEnableCapitalize(true);
            }

            @Override
            public void exitInitialize(ConnectorParser.InitializeContext ctx) {
                kcql.setEnableInitialize(true);
            }

            @Override
            public void exitVersion_number(ConnectorParser.Version_numberContext ctx) {

                final String value = ctx.getText();
                try {
                    int version = Integer.parseInt(value);
                    if (version <= 0) {
                        throw new IllegalArgumentException(value + " is not a valid number for a version.");
                    }
                    kcql.setProjectTo(version);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException(value + " is not a valid number for a version.");
                }
            }

            @Override
            public void exitBuckets_number(ConnectorParser.Buckets_numberContext ctx) {
                bucketsNumber[0] = Integer.parseInt(ctx.getText());
            }

            @Override
            public void exitClusterby_name(ConnectorParser.Clusterby_nameContext ctx) {
                bucketNames.add(ctx.getText());
            }

            @Override
            public void exitBatch_size(ConnectorParser.Batch_sizeContext ctx) {
                final String value = ctx.getText();
                try {
                    int newBatchSize = Integer.parseInt(value);
                    if (newBatchSize <= 0) {
                        throw new IllegalArgumentException(value + " is not a valid number for a batch Size.");
                    }
                    kcql.setBatchSize(newBatchSize);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException(value + " is not a valid number for a batch Size.");
                }
            }

            @Override
            public void exitTimestamp_value(ConnectorParser.Timestamp_valueContext ctx) {
                kcql.setTimestamp(ctx.getText());
            }

            @Override
            public void exitWith_consumer_group_value(ConnectorParser.With_consumer_group_valueContext ctx) {
                String value = escape(ctx.getText());
                kcql.setConsumerGroup(value);
            }

            @Override
            public void exitOffset_partition_inner(ConnectorParser.Offset_partition_innerContext ctx) {
                String value = ctx.getText();
                String[] split = value.split(",");

                if (kcql.partitions == null) {
                    kcql.partitions = new ArrayList<>();
                }


                int partition = Integer.parseInt(split[0]);
                if (split.length == 1) {
                    kcql.partitions.add(new PartitionOffset(partition));
                } else {
                    long offset = Long.parseLong(split[1]);
                    kcql.partitions.add(new PartitionOffset(partition, offset));
                }
            }

            @Override
            public void exitSample_value(ConnectorParser.Sample_valueContext ctx) {
                Integer value = Integer.parseInt(ctx.getText());
                kcql.setSampleCount(value);
            }

            @Override
            public void exitSample_period(ConnectorParser.Sample_periodContext ctx) {
                Integer value = Integer.parseInt(ctx.getText());
                kcql.setSampleRate(value);
            }

            @Override
            public void exitWith_format(ConnectorParser.With_formatContext ctx) {
                FormatType formatType = FormatType.valueOf(ctx.getText().toUpperCase());
                kcql.setFormatType(formatType);
            }

            @Override
            public void exitTag_definition(ConnectorParser.Tag_definitionContext ctx) {
                String[] arr = ctx.getText().trim().split("=");
                if (arr.length == 1) {
                    if (arr[0].trim().length() == 0) {
                        throw new IllegalArgumentException("Invalid syntax for tags. Missing the key");
                    }
                    if (kcql.tags == null) kcql.tags = new ArrayList<>();
                    kcql.tags.add(new Tag(arr[0]));
                } else {
                    if (arr.length != 2) {
                        throw new IllegalArgumentException("Invalid syntax for tags. Can't distinguish the key and value. The format is <key>=format or <key> ['" + ctx.getText() + "']");
                    }
                    if (kcql.tags == null) kcql.tags = new ArrayList<>();
                    kcql.tags.add(new Tag(arr[0], arr[1]));
                }
            }
        });

        try {
            parser.stat();
        } catch (Throwable ex) {
            throw new IllegalArgumentException("Invalid syntax." + ex.getMessage(), ex);
        }

        /*final HashSet<String> pks = new HashSet<>();
        final Iterator<String> iter = kcql.getPrimaryKeys();
        while (iter.hasNext()) {
            pks.add(iter.next());
        }*/

        final HashSet<String> cols = new HashSet<>();
        for (Field alias : kcql.fields.values()) {
            cols.add(alias.getAlias());
        }

    /*
    // This check is necessary only for RDBMS KCQL
    for (String key : pks) {
      if (!cols.contains(key) && !kcql.includeAllFields) {
        throw new IllegalArgumentException(String.format("%s Primary Key needs to appear in the Select clause", key));
      }
    }
    */

        if (!kcql.includeAllFields) {
            final Iterator<String> iterPartitionBy = kcql.getPartitionBy();
            while (iterPartitionBy.hasNext()) {
                final String field = iterPartitionBy.next();
                if (!cols.contains(field)) {
                    throw new IllegalArgumentException(String.format("Partition by field %s is not present in the list of columns specified.", field));
                }
            }
        }

        if (bucketNames.size() > 0 && (bucketsNumber[0] == null || bucketsNumber[0] == 0)) {
            throw new IllegalArgumentException("Invalid bucketing information. Missing the buckets number");
        }
        if (bucketsNumber[0] != null && bucketsNumber[0] > 0 && bucketNames.size() == 0) {
            throw new IllegalArgumentException("Missing bucket columns.");
        }
        if (bucketsNumber[0] != null) {
            final Bucketing bucketing = new Bucketing(bucketNames);
            bucketing.setBucketsNumber(bucketsNumber[0]);

            if (!kcql.includeAllFields) {
                for (final String bucketName : bucketNames) {
                    if (!cols.contains(bucketName)) {
                        throw new IllegalArgumentException(
                                String.format("Bucketing field %s is not present in the selected columns", bucketName));
                    }
                }
            }
            kcql.setBucketing(bucketing);
        }

        String ts = kcql.timestamp;
        if (ts != null) {
            if (TIMESTAMP.compareToIgnoreCase(ts) == 0) {
                kcql.setTimestamp(ts.toLowerCase());
            } else {
                kcql.setTimestamp(ts);
            }
        }

        if (kcql.sampleCount != null && kcql.sampleCount == 0) {
            throw new IllegalArgumentException("Sample count needs to be a positive number greater than zero");
        }

        if (kcql.sampleRate != null && kcql.sampleRate == 0) {
            throw new IllegalArgumentException("Sample rate should be a positive number greater than zero");
        }
        return kcql;
    }

    private static String escape(String value) {
        if (value.startsWith("`")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
