package com.datamountaineer.connector.config;

import com.datamountaineer.connector.config.antlr4.ConnectorLexer;
import com.datamountaineer.connector.config.antlr4.ConnectorParser;
import com.datamountaineer.connector.config.antlr4.ConnectorParserBaseListener;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.OrderedHashSet;

import java.util.*;

/**
 * Holds the configuration for the Kafka Connect topic.
 */
public class Config {

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
    private String docType;
    private String indexSuffix;
    private String incrementalMode;
    private Map<String, FieldAlias> fields = new HashMap<>();
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
    private boolean withUnwrap = false;
    private Integer projectTo;
    private List<Tag> tags;
    private long ttl;
    private String withConverter;

    public void setTTL(long ttl) {
        this.ttl = ttl;
    }

    public long getTTL() {
        return this.ttl;
    }

    private void addIgnoredField(final String ignoredField) {
        if (ignoredField == null || ignoredField.trim().length() == 0) {
            throw new IllegalArgumentException("Invalid ignoredField");
        }
        ignoredFields.add(ignoredField);
    }

    private void addFieldAlias(final FieldAlias fieldAlias) {
        if (fieldAlias == null) {
            throw new IllegalArgumentException("Illegal fieldAlias.");
        }
        fields.put(fieldAlias.getField(), fieldAlias);
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

    private void setWithUnwrap(boolean flag) {
        this.withUnwrap = flag;
    }

    public String getTarget() {
        return target;
    }

    private void setTarget(String target) {
        this.target = target;
    }

    public Iterator<FieldAlias> getFieldAlias() {
        return fields.values().iterator();
    }

    public Iterator<String> getIgnoredField() {
        return ignoredFields.iterator();
    }

    public boolean isIncludeAllFields() {
        return includeAllFields;
    }

    private void setIncludeAllFields(boolean includeAllFields) {
        this.includeAllFields = includeAllFields;
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

    public boolean isWithUnwrap() {
        return this.withUnwrap;
    }

    public String getIncrementalMode() {
        return this.incrementalMode;
    }

    public String getDocType() {
        return this.docType;
    }

    public String getIndexSuffix() {
        return this.indexSuffix;
    }

    public String getWithConverter() {
        return withConverter;
    }

    public static Config parse(final String syntax) {
        final ConnectorLexer lexer = new ConnectorLexer(new ANTLRInputStream(syntax));
        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final ConnectorParser parser = new ConnectorParser(tokens);
        final ArrayList<String> bucketNames = new ArrayList<>();
        final Integer[] bucketsNumber = {null};
        final Config config = new Config();
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

        final String[] columnNameAndAlias = new String[]{null, null};
        final String[] storedAsParameter = {null};

        parser.addParseListener(new ConnectorParserBaseListener() {

            @Override
            public void exitColumn_name(ConnectorParser.Column_nameContext ctx) {
                if (ctx.ID() == null) {
                    if (Objects.equals(ctx.getText(), "*")) {
                        config.setIncludeAllFields(true);
                    }
                    super.exitColumn_name(ctx);
                    return;
                }
                if (columnNameAndAlias[1] != null) {
                    config.addFieldAlias(new FieldAlias(ctx.ID().getText(), columnNameAndAlias[1]));
                    columnNameAndAlias[1] = null;
                } else {
                    config.addFieldAlias(new FieldAlias(ctx.ID().getText()));
                }
            }

            @Override
            public void exitColumn_name_alias(ConnectorParser.Column_name_aliasContext ctx) {
                columnNameAndAlias[1] = ctx.getText();
            }

            @Override
            public void exitPartition_name(ConnectorParser.Partition_nameContext ctx) {
                config.addPartitionByField(ctx.getText());
            }

            @Override
            public void exitDistribute_name(ConnectorParser.Distribute_nameContext ctx) {
                bucketNames.add(ctx.getText());
            }

            @Override
            public void exitTable_name(ConnectorParser.Table_nameContext ctx) {
                config.setTarget(ctx.getText());
            }

            @Override
            public void exitIgnored_name(ConnectorParser.Ignored_nameContext ctx) {
                config.addIgnoredField(ctx.getText());
            }

            @Override
            public void exitTopic_name(ConnectorParser.Topic_nameContext ctx) {
                config.setSource(ctx.getText());
            }

            @Override
            public void exitUpsert_into(ConnectorParser.Upsert_intoContext ctx) {
                config.setWriteMode(WriteModeEnum.UPSERT);
            }

            @Override
            public void exitInsert_into(ConnectorParser.Insert_intoContext ctx) {
                config.setWriteMode(WriteModeEnum.INSERT);
            }

            @Override
            public void exitAutocreate(ConnectorParser.AutocreateContext ctx) {
                config.setAutoCreate(true);
            }

            @Override
            public void exitPk_name(ConnectorParser.Pk_nameContext ctx) {
                config.addPrimaryKey(ctx.getText());
            }

            @Override
            public void exitAutoevolve(ConnectorParser.AutoevolveContext ctx) {
                config.setAutoEvolve(true);
            }

            @Override
            public void exitIndex_suffix(ConnectorParser.Index_suffixContext ctx) {
                config.indexSuffix = ctx.getText();
            }

            @Override
            public void exitDoc_type(ConnectorParser.Doc_typeContext ctx) {
                config.docType = ctx.getText();
            }

            @Override
            public void exitStoreas_type(ConnectorParser.Storeas_typeContext ctx) {
                config.setStoredAs(ctx.getText());
            }

            @Override
            public void exitStoreas_parameter(ConnectorParser.Storeas_parameterContext ctx) {
                String value = ctx.getText();
                for (String key : config.getStoredAsParameters().keySet()) {
                    if (key.compareToIgnoreCase(value) == 0) {
                        throw new IllegalArgumentException(value + " is a duplicated entry in the storeAs parameters list");
                    }
                }
                storedAsParameter[0] = value;
            }

            @Override
            public void exitStoreas_value(ConnectorParser.Storeas_valueContext ctx) {
                config.getStoredAsParameters().put(storedAsParameter[0], ctx.getText());
            }

            @Override
            public void exitCapitalize(ConnectorParser.CapitalizeContext ctx) {
                config.setEnableCapitalize(true);
            }

            @Override
            public void exitInitialize(ConnectorParser.InitializeContext ctx) {
                config.setEnableInitialize(true);
            }

            @Override
            public void exitVersion_number(ConnectorParser.Version_numberContext ctx) {

                final String value = ctx.getText();
                try {
                    int version = Integer.parseInt(value);
                    if (version <= 0) {
                        throw new IllegalArgumentException(value + " is not a valid number for a version.");
                    }
                    config.setProjectTo(version);
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
                    config.setBatchSize(newBatchSize);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException(value + " is not a valid number for a batch Size.");
                }
            }

            @Override
            public void exitTtl_type(ConnectorParser.Ttl_typeContext ctx) {
                final String value = ctx.getText();
                try {
                    long newTTL = Long.parseLong(value);
                    if (newTTL <= 0) {
                        throw new IllegalArgumentException(value + " is not a valid number for a TTL.");
                    }
                    config.setTTL(newTTL);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException(value + " is not a valid number for a TTL.");
                }
            }

            @Override
            public void exitTimestamp_value(ConnectorParser.Timestamp_valueContext ctx) {
                config.setTimestamp(ctx.getText());
            }

            @Override
            public void exitWith_consumer_group_value(ConnectorParser.With_consumer_group_valueContext ctx) {
                config.setConsumerGroup(ctx.getText());
            }

            @Override
            public void exitWith_unwrap_clause(ConnectorParser.With_unwrap_clauseContext ctx) {
                config.setWithUnwrap(true);
            }

            @Override
            public void exitOffset_partition_inner(ConnectorParser.Offset_partition_innerContext ctx) {
                String value = ctx.getText();
                String[] split = value.split(",");

                if (config.partitions == null) {
                    config.partitions = new ArrayList<>();
                }


                int partition = Integer.parseInt(split[0]);
                if (split.length == 1) {
                    config.partitions.add(new PartitionOffset(partition));
                } else {
                    long offset = Long.parseLong(split[1]);
                    config.partitions.add(new PartitionOffset(partition, offset));
                }
            }

            @Override
            public void exitSample_value(ConnectorParser.Sample_valueContext ctx) {
                Integer value = Integer.parseInt(ctx.getText());
                config.setSampleCount(value);
            }

            @Override
            public void exitSample_period(ConnectorParser.Sample_periodContext ctx) {
                Integer value = Integer.parseInt(ctx.getText());
                config.setSampleRate(value);
            }

            @Override
            public void exitWith_format(ConnectorParser.With_formatContext ctx) {
                FormatType formatType = FormatType.valueOf(ctx.getText().toUpperCase());
                config.setFormatType(formatType);
            }

            @Override
            public void exitWith_converter_value(ConnectorParser.With_converter_valueContext ctx) {
                config.withConverter = ctx.getText();
            }

            @Override
            public void exitInc_mode(ConnectorParser.Inc_modeContext ctx) {
                config.incrementalMode = ctx.getText();
            }

            @Override
            public void exitTag_definition(ConnectorParser.Tag_definitionContext ctx) {
                String[] arr = ctx.getText().trim().split("=");
                if (arr.length == 1) {
                    if (arr[0].trim().length() == 0) {
                        throw new IllegalArgumentException("Invalid syntax for tags. Missing the key");
                    }
                    if (config.tags == null) config.tags = new ArrayList<>();
                    config.tags.add(new Tag(arr[0]));
                } else {
                    if (arr.length != 2) {
                        throw new IllegalArgumentException("Invalid syntax for tags. Can't distinguish the key and value. The format is <key>=format or <key> ['" + ctx.getText() + "']");
                    }
                    if (config.tags == null) config.tags = new ArrayList<>();
                    config.tags.add(new Tag(arr[0], arr[1]));
                }
            }
        });

        try {
            parser.stat();
        } catch (Throwable ex) {
            throw new IllegalArgumentException("Invalid syntax." + ex.getMessage(), ex);
        }

        final HashSet<String> pks = new HashSet<>();
        final Iterator<String> iter = config.getPrimaryKeys();
        while (iter.hasNext()) {
            pks.add(iter.next());
        }

        final HashSet<String> cols = new HashSet<>();
        final Iterator<FieldAlias> aliasIterator = config.getFieldAlias();
        while (aliasIterator.hasNext()) {
            cols.add(aliasIterator.next().getAlias());
        }

    /*
    // This check is necessary only for RDBMS KCQL
    for (String key : pks) {
      if (!cols.contains(key) && !config.includeAllFields) {
        throw new IllegalArgumentException(String.format("%s Primary Key needs to appear in the Select clause", key));
      }
    }
    */

        if (!config.includeAllFields) {
            final Iterator<String> iterPartitionBy = config.getPartitionBy();
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

            if (!config.includeAllFields) {
                for (final String bucketName : bucketNames) {
                    if (!cols.contains(bucketName)) {
                        throw new IllegalArgumentException(
                                String.format("Bucketing field %s is not present in the selected columns", bucketName));
                    }
                }
            }
            config.setBucketing(bucketing);
        }

        String ts = config.timestamp;
        if (ts != null) {
            if (TIMESTAMP.compareToIgnoreCase(ts) == 0) {
                config.setTimestamp(ts.toLowerCase());
            } else {
                config.setTimestamp(ts);
            }
        }

        if (config.sampleCount != null && config.sampleCount == 0) {
            throw new IllegalArgumentException("Sample count needs to be a positive number greater than zero");
        }

        if (config.sampleRate != null && config.sampleRate == 0) {
            throw new IllegalArgumentException("Sample rate should be a positive number greater than zero");
        }
        return config;
    }

}
