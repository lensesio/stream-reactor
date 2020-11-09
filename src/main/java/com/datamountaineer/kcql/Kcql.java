package com.datamountaineer.kcql;

import com.datamountaineer.kcql.antlr4.ConnectorLexer;
import com.datamountaineer.kcql.antlr4.ConnectorParser;
import com.datamountaineer.kcql.antlr4.ConnectorParserBaseListener;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Parsing support for Kafka Connect Query Language.
 */
public class Kcql {

  public final static String TIMESTAMP = "sys_time()";
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
  private List<Field> keyFields = new ArrayList<>();
  private List<Field> headerFields = new ArrayList<>();
  private List<Field> ignoredFields = new ArrayList<>();
  private List<Field> primaryKeys = new ArrayList<>();
  private List<String> partitionBy = new ArrayList<>();
  private int retries = 1;
  private int limit = 0;
  private int batchSize;
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
  private long ttl;
  private String withType;
  private String withJmsSelector;
  private String dynamicTarget;
  private List<String> withKeys = null;
  private String keyDelimeter = ".";
  private TimeUnit timestampUnit = TimeUnit.MILLISECONDS;
  private String pipeline;
  private CompressionType compression;
  private String subscription;
  private String partitioner;
  private String withRegex;
  private long withFlushInterval;
  private long withFlushSize;
  private long withFlushCount;
  private SchemaEvolution withSchemaEvolution = SchemaEvolution.MATCH;
  private String withTableLocation;
  private boolean withOverwrite;
  private PartitioningStrategy withPartitioningStrategy;
  private int delay;
  private String withSession;
  private boolean withAck = false;
  private boolean withEncodeBase64 = false;
  private long withLockTime = -1;

  public boolean getWithEncodeBase64() { return this.withEncodeBase64; }

  public void setWithEncodeBase64(boolean encode) { this.withEncodeBase64 = encode; }

  public boolean getWithAck() { return this.withAck; }

  public void setWithAck(boolean ack) {
    this.withAck = ack;
  }

  public String getWithSession() { return this.withSession; }

  public String getWithPartitioner() {
    return this.partitioner;
  }

  public void setWithPartitioner(String name) {
    this.partitioner = name;
  }

  public String getWithSubscription() {
    return this.subscription;
  }

  public void SetWithSubscription(String name) {
    this.subscription = name;
  }

  public int getWithDelay() {
    return this.delay;
  }

  public void setWithDelay(Integer delay) {
    this.delay = delay;
  }

  public void setWithCompression(CompressionType compression) {
    this.compression = compression;
  }

  public CompressionType getWithCompression() {
    return this.compression;
  }

  public void setTTL(long ttl) {
    this.ttl = ttl;
  }

  public long getTTL() {
    return this.ttl;
  }

  public long getWithLockTime() { return this.withLockTime; }

  public void setWithLockTime(long lock) { this.withLockTime = lock;}

  private void addField(final Field field) {
    if (field == null) {
      throw new IllegalArgumentException("Illegal fieldAlias.");
    }
    if (fieldExists(field)) {
      throw new IllegalArgumentException(String.format("Field %s has already been defined", field.getName()));
    }
    fields.add(field);
  }


  private void addKeyField(final Field field) {
    if (field == null) {
      throw new IllegalArgumentException("Illegal fieldAlias.");
    }
    if (fieldExists(field)) {
      throw new IllegalArgumentException(String.format("Key field %s has already been defined", field.getName()));
    }
    keyFields.add(field);
  }

  private void addHeaderField(final Field field) {
    if (field == null) {
      throw new IllegalArgumentException("Illegal fieldAlias.");
    }
    if (fieldExists(field)) {
      throw new IllegalArgumentException(String.format("Header field %s has already been defined", field.getName()));
    }
    headerFields.add(field);
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

  private boolean keyFieldExists(final Field newField) {
    for (Field field : keyFields) {
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

  private boolean headerFieldExists(final Field newField) {
    for (Field field : headerFields) {
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

    /*private void addPrimaryKey(final String primaryKey) {
        if (primaryKey == null || primaryKey.trim().length() == 0) {
            throw new IllegalArgumentException("Invalid primaryKey.");
        }
        if (primaryKeys.contains(primaryKey)) {
            throw new IllegalArgumentException(String.format("%s has already been defined", primaryKey));
        }
        primaryKeys.add(primaryKey);
    }*/

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

  public List<Field> getKeyFields() {
    return keyFields;
    //return new ArrayList<>(fields);
  }

  public List<Field> getHeaderFields() {
    return headerFields;
    //return new ArrayList<>(fields);
  }

  public List<Field> getIgnoredFields() {
    //return new HashSet<>(ignoredFields);
    return ignoredFields;
  }

  public WriteModeEnum getWriteMode() {
    return writeMode;
  }

  public List<Field> getPrimaryKeys() {
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

  public List<PartitionOffset> getPartitionOffset() {
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

  public List<String> getWithKeys() {
    return withKeys;
  }

  public String getKeyDelimeter() {
    return keyDelimeter;
  }

  public boolean hasRetainStructure() {
    return retainStructure;
  }

  public boolean isUnwrapping() {
    return unwrapping;
  }

  public String getWithType() {
    return this.withType;
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

  public String getWithJmsSelector() {
    return withJmsSelector;
  }

  public String getPipeline() {
    return pipeline;
  }

  public String getWithRegex() {
    return withRegex;
  }

  private void setWithRegex(String withRegex) {
    this.withRegex = withRegex;
  }

  public long getWithFlushInterval() {
    return withFlushInterval;
  }

  public long getWithFlushSize() {
    return withFlushSize;
  }

  private void setWithFlushCount(long withFlushCount) {
    this.withFlushCount = withFlushCount;
  }

  public long getWithFlushCount() {
    return withFlushCount;
  }

  private void setDynamicTarget(String dynamicTarget) {
    this.dynamicTarget = dynamicTarget;
  }

  public String getDynamicTarget() {
    return dynamicTarget;
  }

  public TimeUnit getTimestampUnit() {
    return timestampUnit;
  }

  private void setTimestampUnit(TimeUnit timestampUnit) {
    this.timestampUnit = timestampUnit;
  }

  private void setWithFlushInterval(long withFlushInterval) {
    this.withFlushInterval = withFlushInterval;
  }

  private void setWithFlushSize(long withFlushSize) {
    this.withFlushSize = withFlushSize;
  }

  public SchemaEvolution getWithSchemaEvolution() {
    return withSchemaEvolution;
  }

  private void setWithSchemaEvolution(SchemaEvolution withSchemaEvolution) {
    this.withSchemaEvolution = withSchemaEvolution;
  }

  public String getWithTableLocation() {
    return withTableLocation;
  }

  private void setWithTableLocation(String withTableLocation) {
    this.withTableLocation = withTableLocation;
  }

  public boolean getWithOverwrite() {
    return withOverwrite;
  }

  private void setWithOverwrite(boolean withOverwrite) {
    this.withOverwrite = withOverwrite;
  }

  public PartitioningStrategy getWithPartitioningStrategy() {
    return withPartitioningStrategy;
  }

  private void setWithPartitioningStrategy(PartitioningStrategy withPartitioningStrategy) {
    this.withPartitioningStrategy = withPartitioningStrategy;
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

    final String[] tagValue = {null};
    final String[] tagKey = {null};

    parser.addParseListener(new ConnectorParserBaseListener() {

      @Override
      public void exitWith_session_value(ConnectorParser.With_session_valueContext ctx) {
        kcql.withSession = ctx.getText();
      }

      @Override
      public void exitWith_subscription_value(ConnectorParser.With_subscription_valueContext ctx) {
        kcql.subscription = unescape(ctx.getText());
      }

      @Override
      public void exitWith_partitioner_value(ConnectorParser.With_partitioner_valueContext ctx) {
        kcql.partitioner = unescape(ctx.getText());
      }

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
      public void exitWith_type_value(ConnectorParser.With_type_valueContext ctx) {
        kcql.withType = unescape(ctx.getText());
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
          List<String> cleanedParent = null;

          if (field.toString().startsWith("_key.")) {
            trimParentField(nestedFieldsBuffer);
            cleanedParent = nestedFieldsBuffer;
            kcql.addKeyField(Field.from(field.getName(), field.getAlias(), cleanedParent));
          } else if (field.toString().startsWith("_header.")) {
            trimParentField(nestedFieldsBuffer);
            cleanedParent = nestedFieldsBuffer;
            kcql.addHeaderField(Field.from(field.getName(), field.getAlias(), cleanedParent));
          } else {
            kcql.addField(field);
          }
        }
      }

      private void trimParentField(List<String> parents) {
        if (nestedFieldsBuffer.size() > 0) {
          nestedFieldsBuffer.remove(0);
        }
      }

      @Override
      public void exitWith_compression_type(ConnectorParser.With_compression_typeContext ctx) {
        String type = unescape(ctx.getText()).toUpperCase();
        CompressionType compressionType = CompressionType.valueOf(type);
        kcql.setWithCompression(compressionType);
      }

      @Override
      public void exitWith_delay_value(ConnectorParser.With_delay_valueContext ctx) {
        kcql.delay = Integer.parseInt(ctx.getText());
      }

      @Override
      public void exitDoc_type(ConnectorParser.Doc_typeContext ctx) {
        kcql.docType = unescape(ctx.getText());
      }

      @Override
      public void exitWith_converter_value(ConnectorParser.With_converter_valueContext ctx) {
        kcql.withConverter = unescape(ctx.getText());
      }

      @Override
      public void exitJms_selector_value(ConnectorParser.Jms_selector_valueContext ctx) {
        kcql.withJmsSelector = unescape(ctx.getText());
      }

      @Override
      public void exitIndex_suffix(ConnectorParser.Index_suffixContext ctx) {
        kcql.indexSuffix = unescape(ctx.getText());
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
        kcql.target = unescape(ctx.getText());
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
        kcql.source = unescape(ctx.getText());
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
      public void exitUpdate_into(ConnectorParser.Update_intoContext ctx) {
        kcql.writeMode = WriteModeEnum.UPDATE;
      }

      @Override
      public void exitAutocreate(ConnectorParser.AutocreateContext ctx) {
        kcql.autoCreate = true;
      }

      @Override
      public void enterPk_name(ConnectorParser.Pk_nameContext ctx) {
        nestedFieldsBuffer.clear();
      }

      @Override
      public void exitPk_name(ConnectorParser.Pk_nameContext ctx) {
        List<String> parentFields = null;
        String name = nestedFieldsBuffer.get(nestedFieldsBuffer.size() - 1);
        nestedFieldsBuffer.remove(nestedFieldsBuffer.size() - 1);

        if (nestedFieldsBuffer.size() > 0) {
          parentFields = nestedFieldsBuffer;
        }

        Field field = Field.from(name, parentFields);
        kcql.primaryKeys.add(field);
        //kcql.addPrimaryKey(ctx.getText());
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
      public void exitTtl_type(ConnectorParser.Ttl_typeContext ctx) {
        final String value = ctx.getText();
        try {
          long newTTL = Long.parseLong(value);
          if (newTTL <= 0) {
            throw new IllegalArgumentException(value + " is not a valid number for a TTL.");
          }
          kcql.setTTL(newTTL);
        } catch (NumberFormatException ex) {
          throw new IllegalArgumentException(value + " is not a valid number for a TTL.");
        }
      }

      @Override
      public void exitLock_time_type(ConnectorParser.Lock_time_typeContext ctx) {
        final String value = ctx.getText();
        try {
          long lockTime = Long.parseLong(value);
          if (lockTime <= 0) {
            throw new IllegalArgumentException(value + " is not a valid number for a WITH_LOCK_TIME.");
          }
          kcql.setWithLockTime(lockTime);
        } catch (NumberFormatException ex) {
          throw new IllegalArgumentException(value + " is not a valid number for a WITH_LOCK_TIME.");
        }
      }

      @Override
      public void exitTimestamp_value(ConnectorParser.Timestamp_valueContext ctx) {
        kcql.timestamp = ctx.getText();
      }

      @Override
      public void exitWith_consumer_group_value(ConnectorParser.With_consumer_group_valueContext ctx) {
        String value = unescape(ctx.getText());
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
      public void exitTimestamp_unit_value(ConnectorParser.Timestamp_unit_valueContext ctx) {
        String value = ctx.getText().toUpperCase();
        try {
          kcql.setTimestampUnit(TimeUnit.valueOf(value));
        } catch (Throwable t) {
          TimeUnit[] units = TimeUnit.values();
          StringBuilder sb = new StringBuilder();
          sb.append(units[0].toString());
          for (int i = 1; i < units.length; ++i) {
            sb.append(",");
            sb.append(units[i].toString());
          }
          throw new IllegalArgumentException(("Invalid 'TIMESTAMPUNIT'. Available values are : " + sb.toString()));
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
      public void exitWith_target_value(ConnectorParser.With_target_valueContext ctx) {
        kcql.setDynamicTarget(ctx.getText());
      }

      @Override
      public void exitTag_value(ConnectorParser.Tag_valueContext ctx) {
        tagValue[0] = ctx.getText();
      }

      @Override
      public void exitTag_key(ConnectorParser.Tag_keyContext ctx) {
        if (ctx.getText().trim().endsWith(".")) {
          throw new IllegalArgumentException("Invalid syntax for tags. Field selection can not end with '.'");
        }
        tagKey[0] = ctx.getText();
      }

      @Override
      public void exitTag_definition(ConnectorParser.Tag_definitionContext ctx) {
        String txt = ctx.getText();
        Tag.TagType type = Tag.TagType.DEFAULT;
        if (tagValue[0] != null) {
          String tmp = txt.replace(tagKey[0], "").trim();
          if (tmp.startsWith("=")) {
            type = Tag.TagType.CONSTANT;
          } else if (tmp.toLowerCase().startsWith("as")) {
            type = Tag.TagType.ALIAS;
          } else {
            throw new IllegalArgumentException("Invalid syntax for tags. Needs to be 'tag1 [as x]' or 'tag1' or 'tag1 = constant'");
          }
        }

        if (kcql.tags == null) kcql.tags = new ArrayList<>();
        kcql.tags.add(new Tag(tagKey[0], tagValue[0], type));
        tagKey[0] = null;
        tagValue[0] = null;
      }

      @Override
      public void exitWith_key_value(ConnectorParser.With_key_valueContext ctx) {
        String key = ctx.getText();
        if (kcql.withKeys == null) {
          kcql.withKeys = new ArrayList<>();
        }
        kcql.withKeys.add(unescape(key));
      }

      @Override
      public void exitKey_delimiter_value(ConnectorParser.Key_delimiter_valueContext ctx) {
        kcql.keyDelimeter = ctx.getText().replace("`", "");
        if (kcql.keyDelimeter.trim().length() == 0) {
          throw new IllegalArgumentException("Invalid key delimiter. Needs to be a non empty string.");
        }
      }

      @Override
      public void exitPipeline_value(ConnectorParser.Pipeline_valueContext ctx) {
        kcql.pipeline = unescape(ctx.getText());
      }

      @Override
      public void exitWith_regex_value(ConnectorParser.With_regex_valueContext ctx) {
        kcql.withRegex = unescape(ctx.getText());
      }


      @Override
      public void exitWith_flush_bytes_value(ConnectorParser.With_flush_bytes_valueContext ctx) {
        try {
          long size = Long.parseLong(ctx.getText());
          if (size <= 0) {
            throw new IllegalArgumentException("Invalid value specified for WITH_FLUSH_SIZE. Expecting a LONG number greater than 0.");
          }
          kcql.setWithFlushSize(size);
        } catch (NumberFormatException ex) {
          throw new IllegalArgumentException("Invalid value specified for WITH_FLUSH_SIZE. Expecting a LONG number greater than 0.");
        }
      }

      @Override
      public void exitWith_flush_interval_value(ConnectorParser.With_flush_interval_valueContext ctx) {
        try {
          long interval = Long.parseLong(ctx.getText());
          if (interval <= 0) {
            throw new IllegalArgumentException("Invalid value specified for WITH_FLUSH_INTERVAL. Expecting a LONG number greater than 0.");
          }
          kcql.setWithFlushInterval(interval);
        } catch (NumberFormatException ex) {
          throw new IllegalArgumentException("Invalid value specified for WITH_FLUSH_INTERVAL. Expecting a LONG number greater than 0.");
        }
      }

      @Override
      public void exitWith_flush_records_value(ConnectorParser.With_flush_records_valueContext ctx) {
        try {
          long interval = Long.parseLong(ctx.getText());
          if (interval <= 0) {
            throw new IllegalArgumentException("Invalid value specified for WITH_FLUSH_COUNT. Expecting a LONG number greater than 0.");
          }
          kcql.setWithFlushCount(interval);
        } catch (NumberFormatException ex) {
          throw new IllegalArgumentException("Invalid value specified for WITH_FLUSH_COUNT. Expecting a LONG number greater than 0.");
        }
      }

      @Override
      public void exitWith_schema_evolution_value(ConnectorParser.With_schema_evolution_valueContext ctx) {
        String value = ctx.getText().toUpperCase();
        try {
          SchemaEvolution schemaEvolution = Enum.valueOf(SchemaEvolution.class, value);
          kcql.setWithSchemaEvolution(schemaEvolution);
        } catch (Throwable t) {
          throw new IllegalArgumentException("Invalid value specified for WITH_SCHEMA_EVOLUTION. Expecting one of the values:" + EnumsHelper.mkString(SchemaEvolution.values()));
        }
      }

      @Override
      public void exitWith_table_location_value(ConnectorParser.With_table_location_valueContext ctx) {
        kcql.setWithTableLocation(unescape(ctx.getText()));
      }

      @Override
      public void exitWith_overwrite_clause(ConnectorParser.With_overwrite_clauseContext ctx) {
        kcql.setWithOverwrite(true);
      }

      @Override
      public void exitWith_partitioning_value(ConnectorParser.With_partitioning_valueContext ctx) {
        //_dynamic_ and _static_
        String value = ctx.getText().toUpperCase();
        try {
          PartitioningStrategy strategy = Enum.valueOf(PartitioningStrategy.class, value);
          kcql.setWithPartitioningStrategy(strategy);
        } catch (Throwable t) {
          throw new IllegalArgumentException("Invalid value specified for WITH_SCHEMA_EVOLUTION. Expecting one of the values:" + EnumsHelper.mkString(PartitioningStrategy.values()));
        }
      }

      @Override
      public void exitWith_ack_clause(ConnectorParser.With_ack_clauseContext ctx) {
        kcql.setWithAck(true);
      }


      @Override
      public void exitWith_encode_base64(ConnectorParser.With_encode_base64Context ctx) {
        kcql.setWithEncodeBase64(true);
      }
    });

    try {
      parser.stat();
    } catch (Throwable ex) {
      throw new IllegalArgumentException("Invalid syntax." + ex.getMessage(), ex);
    }

    final HashSet<String> cols = new HashSet<>();
    for (Field alias : kcql.fields) {
      cols.add(alias.getAlias());
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

  private static String unescape(String value) {
    if (value.startsWith("`") && value.endsWith("`")) {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }
}
