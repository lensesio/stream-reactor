package com.datamountaineer.connector.config;

import com.datamountaineer.connector.config.antlr4.ConnectorLexer;
import com.datamountaineer.connector.config.antlr4.ConnectorParser;
import com.datamountaineer.connector.config.antlr4.ConnectorParserBaseListener;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Created by stepi on 29/05/16.
 */
public class Config {
  private boolean includeAllFields;
  private WriteModeEnum writeMode;
  private String topic;
  private String table;
  private Map<String, FieldAlias> fields = new HashMap<>();
  private Set<String> ignoredFields = new HashSet<>();

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

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
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
      public void exitColumn_list(ConnectorParser.Column_listContext ctx) {
        for (ConnectorParser.Column_nameContext cn_ctx : ctx.column_name()) {
          ConnectorParser.Column_name_aliasContext cn_alias_ctx = cn_ctx.column_name_alias();
          if (cn_alias_ctx != null) {
            config.addFieldAlias(new FieldAlias(cn_alias_ctx.getText(), ctx.getText()));
          } else {
            config.addFieldAlias(new FieldAlias(ctx.getText(), ctx.getText()));
          }
        }
      }

      @Override
      public void exitColumn_name(ConnectorParser.Column_nameContext ctx) {
        if (ctx.ID() == null) {
          //for *
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
        config.setTable(ctx.getText());
      }

      @Override
      public void exitIgnored_name(ConnectorParser.Ignored_nameContext ctx) {
        config.addIgnoredField(ctx.getText());
      }

      @Override
      public void exitTopic_name(ConnectorParser.Topic_nameContext ctx) {
        config.setTopic(ctx.getText());
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
      public void exitSelect_clause(ConnectorParser.Select_clauseContext ctx) {
        for (ParseTree pt : ctx.column_list_clause().children) {
          if (Objects.equals(pt.getText(), "*")) {
            config.setIncludeAllFields(true);
            break;
          }
        }
      }
    });
    parser.stat();

    return config;
  }
}
