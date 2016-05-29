package com.datamountaineer.connector.config;

import com.datamountaineer.connector.config.antlr4.ConnectorLexer;
import com.datamountaineer.connector.config.antlr4.ConnectorParser;
import com.datamountaineer.connector.config.antlr4.ConnectorParserBaseListener;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.testng.collections.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Holds the configuration for the Kafka Connect topic.
 */
public class Config {

  /**
   * Returns true if all payload fields should be included; false - otherwise
   */
  private boolean includeAllFields;
  private boolean autoCreate;
  private WriteModeEnum writeMode;
  private String source;
  private String target;
  private Map<String, FieldAlias> fields = new HashMap<>();
  private Set<String> ignoredFields = new HashSet<>();
  private Set<String> primaryKeys = new HashSet<>();

  public void addIgnoredField(final String ignoredField) {
    if (ignoredField == null || ignoredField.trim().length() == 0) {
      throw new IllegalArgumentException("Invalid ignoredField");
    }
    ignoredFields.add(ignoredField);
  }

  public void addFieldAlias(final FieldAlias fieldAlias) {
    if (fieldAlias == null) {
      throw new IllegalArgumentException("Illegal fieldAlias.");
    }
    fields.put(fieldAlias.getField(), fieldAlias);
  }

  public void addPrimaryKey(final String primaryKey) {
    if (primaryKey == null || primaryKey.trim().length() == 0) {
      throw new IllegalArgumentException("Invalid primaryKey.");
    }
    primaryKeys.add(primaryKey);
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getTarget() {
    return target;
  }

  public void setTarget(String target) {
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

  public void setIncludeAllFields(boolean includeAllFields) {
    this.includeAllFields = includeAllFields;
  }

  public WriteModeEnum getWriteMode() {
    return writeMode;
  }

  public void setWriteMode(WriteModeEnum writeMode) {
    this.writeMode = writeMode;
  }

  public Iterator<String> getPrimaryKeys() {
    return primaryKeys.iterator();
  }

  public static Config parse(final String syntax) {
    final ConnectorLexer lexer = new ConnectorLexer(new ANTLRInputStream(syntax));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final ConnectorParser parser = new ConnectorParser(tokens);

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
    });
    try {
      parser.stat();
    } catch (RecognitionException ex) {
      throw new IllegalArgumentException("Invalid syntax." + ex.getMessage(), ex);
    }
    if (config.isAutoCreate()) {
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

      if (cols.size() == 0 && pks.size() > 0) {
        throw new IllegalArgumentException("Invalid syntax. Primary Keys are specified but they are not present in the select clause.");
      }
      for (String key : pks) {
        if (!cols.contains(key)) {
          throw new IllegalArgumentException(String.format("%s Primary Key needs to appear in the Select clause", key));
        }
      }
    }
    return config;
  }

  public boolean isAutoCreate() {
    return autoCreate;
  }

  public void setAutoCreate(boolean autoCreate) {
    this.autoCreate = autoCreate;
  }
}
