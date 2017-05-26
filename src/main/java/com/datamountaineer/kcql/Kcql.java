package com.datamountaineer.kcql;

import com.datamountaineer.kcql.antlr4.ConnectorLexer;
import com.datamountaineer.kcql.antlr4.ConnectorParser;
import com.datamountaineer.kcql.antlr4.ConnectorParserBaseListener;
import org.antlr.v4.runtime.*;
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
    private String docType;
    private String indexSuffix;
    private String incrementalMode;
    private List<Field> fields = new ArrayList<>();
    private List<Field> ignoredFields = new ArrayList<>();
    private List<String> primaryKeys = new ArrayList<>();
    private List<String> partitionBy = new ArrayList<>();
    private int retries = 1;
    private int limit = 5;
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
    private boolean unwrapping = false;
    private Integer projectTo;
    private List<Tag> tags;
    private boolean retainStructure = false;
    private String withConverter;


    private void addField(final Field field) {
        if (field == null) {
            throw new IllegalArgumentException("Illegal fieldAlias.");
        }
        if (fieldExists(field)) {
            throw new IllegalArgumentException(String.format("Field %s has already been defined", field.getName()));
        }
        fields.add(field);
    }

    private boolean fieldExists(final Field newField) {
        for (Field field : fields) {
            if (!field.getName().equals(newField.getName()) ||
                    !field.getFieldType().equals(newField.getFieldType())) {
                continue;
            }
            if (!field.hasParents() && !newField.hasParents()) {
                return true;
            }
            if (field.hasParents() && newField.hasParents()) {
                if (field.getParentFields().equals(newField.hasParents())) {
                    return true;
                }
            }
        }
        return false;
    }

    private void addPrimaryKey(final String primaryKey) {
        if (primaryKey == null || primaryKey.trim().length() == 0) {
            throw new IllegalArgumentException("Invalid primaryKey.");
        }
        if (primaryKeys.contains(primaryKey)) {
            throw new IllegalArgumentException(String.format("%s has already been defined", primaryKey));
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

    public String getTarget() {
        return target;
    }

    public List<Field> getFields() {
        return fields;
        //return new ArrayList<>(fields);
    }

    public List<Field> getIgnoredFields() {
        //return new HashSet<>(ignoredFields);
        return ignoredFields;
    }

    public boolean isIncludeAllFields() {
        return includeAllFields;
    }

    public WriteModeEnum getWriteMode() {
        return writeMode;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;//.iterator();
    }

    public Bucketing getBucketing() {
        return bucketing;
    }

    public String getTimestamp() {
        return this.timestamp;
    }

    public String getStoredAs() {
        return storedAs;
    }

    public Map<String, String> getStoredAsParameters() {
        return storedAsParameters;
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

    public Integer getSampleRate() {
        return sampleRate;
    }

    public FormatType getFormatType() {
        return formatType;
    }

    public Integer getProjectTo() {
        return projectTo;
    }

    public boolean isAutoCreate() {
        return autoCreate;
    }

    public int getLimit() {
        return limit;
    }

    public int getRetries() {
        return retries;
    }

    public boolean isAutoEvolve() {
        return autoEvolve;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isEnableCapitalize() {
        return enableCapitalize;
    }

    public boolean isInitialize() {
        return initialize;
    }

    public Iterator<String> getPartitionBy() {
        return partitionBy.iterator();
    }

    public List<Tag> getTags() {
        return tags;
    }

    public boolean hasRetainStructure() {
        return retainStructure;
    }

    public boolean isUnwrapping() {
        return unwrapping;
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

        final boolean[] isWithinIgnore = {false};
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
            public void exitWith_unwrap_clause(ConnectorParser.With_unwrap_clauseContext ctx) {
                kcql.unwrapping = true;
            }

            @Override
            public void exitWith_structure(ConnectorParser.With_structureContext ctx) {
                kcql.retainStructure = true;
            }

            @Override
            public void exitLimit_value(ConnectorParser.Limit_valueContext ctx) {
                try {
                    int limit = Integer.parseInt(ctx.INT().getText());
                    if (limit < 1)
                        throw new IllegalArgumentException("Invalid limit specified. Needs to be an integer greater than zero");
                    kcql.limit = limit;
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException("Invalid limit specified(" + ctx.INT().getText() + "). Needs to be an integer greater than zero");
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
                    kcql.includeAllFields = true;
                    Field field = new Field("*", FieldType.VALUE, null);
                    kcql.addField(field);
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

                if (isWithinIgnore[0]) {
                    kcql.ignoredFields.add(field);
                } else {
                    if ("*".equals(field.getName()) && !field.hasParents() && field.getFieldType() != FieldType.KEY) {
                        kcql.includeAllFields = true;
                    }
                    kcql.addField(field);
                }
            }


            @Override
            public void exitDoc_type(ConnectorParser.Doc_typeContext ctx) {
                kcql.docType = escape(ctx.getText());
            }

            @Override
            public void exitWith_converter_value(ConnectorParser.With_converter_valueContext ctx) {
                kcql.withConverter = escape(ctx.getText());
            }

            @Override
            public void exitIndex_suffix(ConnectorParser.Index_suffixContext ctx) {
                kcql.indexSuffix = escape(ctx.getText());
            }

            @Override
            public void exitInc_mode(ConnectorParser.Inc_modeContext ctx) {
                kcql.incrementalMode = ctx.getText();
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
                kcql.target = escape(ctx.getText());
            }

            @Override
            public void enterWith_ignore(ConnectorParser.With_ignoreContext ctx) {
                isWithinIgnore[0] = true;
            }

            @Override
            public void exitWith_ignore(ConnectorParser.With_ignoreContext ctx) {
                isWithinIgnore[0] = false;
            }

            @Override
            public void exitTopic_name(ConnectorParser.Topic_nameContext ctx) {
                kcql.source = escape(ctx.getText());
            }

            @Override
            public void exitUpsert_into(ConnectorParser.Upsert_intoContext ctx) {
                kcql.writeMode = WriteModeEnum.UPSERT;
            }

            @Override
            public void exitInsert_into(ConnectorParser.Insert_intoContext ctx) {
                kcql.writeMode = WriteModeEnum.INSERT;
            }

            @Override
            public void exitAutocreate(ConnectorParser.AutocreateContext ctx) {
                kcql.autoCreate = true;
            }

            @Override
            public void exitPk_name(ConnectorParser.Pk_nameContext ctx) {
                kcql.addPrimaryKey(ctx.getText());
            }

            @Override
            public void exitAutoevolve(ConnectorParser.AutoevolveContext ctx) {
                kcql.autoEvolve = true;
            }

            @Override
            public void exitStoreas_type(ConnectorParser.Storeas_typeContext ctx) {
                kcql.storedAs = ctx.getText();
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
                kcql.enableCapitalize = true;
            }

            @Override
            public void exitInitialize(ConnectorParser.InitializeContext ctx) {
                kcql.initialize = true;
            }

            @Override
            public void exitVersion_number(ConnectorParser.Version_numberContext ctx) {

                final String value = ctx.getText();
                try {
                    int version = Integer.parseInt(value);
                    if (version <= 0) {
                        throw new IllegalArgumentException(value + " is not a valid number for a version.");
                    }
                    kcql.projectTo = version;
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
                    kcql.batchSize = newBatchSize;
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException(value + " is not a valid number for a batch Size.");
                }
            }

            @Override
            public void exitTimestamp_value(ConnectorParser.Timestamp_valueContext ctx) {
                kcql.timestamp = ctx.getText();
            }

            @Override
            public void exitWith_consumer_group_value(ConnectorParser.With_consumer_group_valueContext ctx) {
                String value = escape(ctx.getText());
                kcql.consumerGroup = value;
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
                kcql.sampleCount = value;
            }

            @Override
            public void exitSample_period(ConnectorParser.Sample_periodContext ctx) {
                Integer value = Integer.parseInt(ctx.getText());
                kcql.sampleRate = value;
            }

            @Override
            public void exitWith_format(ConnectorParser.With_formatContext ctx) {
                FormatType formatType = FormatType.valueOf(ctx.getText().toUpperCase());
                kcql.formatType = formatType;
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
        for (Field alias : kcql.fields) {
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
            kcql.bucketing = bucketing;
        }

        String ts = kcql.timestamp;
        if (ts != null) {
            if (TIMESTAMP.compareToIgnoreCase(ts) == 0) {
                kcql.timestamp = ts.toLowerCase();
            } else {
                kcql.timestamp = ts;
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
