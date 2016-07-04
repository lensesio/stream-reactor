// Generated from ConnectorParser.g4 by ANTLR 4.5.3
package com.datamountaineer.connector.config.antlr4;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ConnectorParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		INSERT=1, UPSERT=2, INTO=3, SELECT=4, FROM=5, IGNORE=6, AS=7, AUTOCREATE=8, 
		AUTOEVOLVE=9, CLUSTERBY=10, BUCKETS=11, BATCH=12, CAPITALIZE=13, PARTITIONBY=14, 
		DISTRIBUTEBY=15, PK=16, INT=17, ASTERISK=18, COMMA=19, DOT=20, ID=21, 
		TOPICNAME=22, NEWLINE=23, WS=24, EQUAL=25;
	public static final int
		RULE_stat = 0, RULE_into = 1, RULE_pk = 2, RULE_insert_into = 3, RULE_upsert_into = 4, 
		RULE_upsert_pk_into = 5, RULE_sql_action = 6, RULE_schema_name = 7, RULE_select_clause = 8, 
		RULE_topic_name = 9, RULE_table_name = 10, RULE_column_name = 11, RULE_column_name_alias = 12, 
		RULE_column_list = 13, RULE_from_clause = 14, RULE_ignored_name = 15, 
		RULE_ignore_clause = 16, RULE_pk_name = 17, RULE_primary_key_list = 18, 
		RULE_autocreate = 19, RULE_autoevolve = 20, RULE_batch_size = 21, RULE_batching = 22, 
		RULE_capitalize = 23, RULE_partition_name = 24, RULE_partition_list = 25, 
		RULE_partitionby = 26, RULE_distribute_name = 27, RULE_distribute_list = 28, 
		RULE_distributeby = 29, RULE_buckets_number = 30, RULE_clusterby_name = 31, 
		RULE_clusterby_list = 32, RULE_clusterby = 33;
	public static final String[] ruleNames = {
		"stat", "into", "pk", "insert_into", "upsert_into", "upsert_pk_into", 
		"sql_action", "schema_name", "select_clause", "topic_name", "table_name", 
		"column_name", "column_name_alias", "column_list", "from_clause", "ignored_name", 
		"ignore_clause", "pk_name", "primary_key_list", "autocreate", "autoevolve", 
		"batch_size", "batching", "capitalize", "partition_name", "partition_list", 
		"partitionby", "distribute_name", "distribute_list", "distributeby", "buckets_number", 
		"clusterby_name", "clusterby_list", "clusterby"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, "'*'", "','", "'.'", null, null, null, 
		null, "'='"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "PARTITIONBY", 
		"DISTRIBUTEBY", "PK", "INT", "ASTERISK", "COMMA", "DOT", "ID", "TOPICNAME", 
		"NEWLINE", "WS", "EQUAL"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "ConnectorParser.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public ConnectorParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class StatContext extends ParserRuleContext {
		public Select_clauseContext select_clause() {
			return getRuleContext(Select_clauseContext.class,0);
		}
		public StatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_stat; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterStat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitStat(this);
		}
	}

	public final StatContext stat() throws RecognitionException {
		StatContext _localctx = new StatContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_stat);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(68);
			select_clause();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntoContext extends ParserRuleContext {
		public TerminalNode INTO() { return getToken(ConnectorParser.INTO, 0); }
		public IntoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_into; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterInto(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitInto(this);
		}
	}

	public final IntoContext into() throws RecognitionException {
		IntoContext _localctx = new IntoContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_into);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(70);
			match(INTO);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PkContext extends ParserRuleContext {
		public TerminalNode PK() { return getToken(ConnectorParser.PK, 0); }
		public PkContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pk; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterPk(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitPk(this);
		}
	}

	public final PkContext pk() throws RecognitionException {
		PkContext _localctx = new PkContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_pk);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(72);
			match(PK);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Insert_intoContext extends ParserRuleContext {
		public TerminalNode INSERT() { return getToken(ConnectorParser.INSERT, 0); }
		public IntoContext into() {
			return getRuleContext(IntoContext.class,0);
		}
		public Insert_intoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert_into; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterInsert_into(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitInsert_into(this);
		}
	}

	public final Insert_intoContext insert_into() throws RecognitionException {
		Insert_intoContext _localctx = new Insert_intoContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_insert_into);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(74);
			match(INSERT);
			setState(75);
			into();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Upsert_intoContext extends ParserRuleContext {
		public TerminalNode UPSERT() { return getToken(ConnectorParser.UPSERT, 0); }
		public IntoContext into() {
			return getRuleContext(IntoContext.class,0);
		}
		public Upsert_intoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_upsert_into; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterUpsert_into(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitUpsert_into(this);
		}
	}

	public final Upsert_intoContext upsert_into() throws RecognitionException {
		Upsert_intoContext _localctx = new Upsert_intoContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_upsert_into);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(77);
			match(UPSERT);
			setState(78);
			into();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Upsert_pk_intoContext extends ParserRuleContext {
		public TerminalNode UPSERT() { return getToken(ConnectorParser.UPSERT, 0); }
		public PkContext pk() {
			return getRuleContext(PkContext.class,0);
		}
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public IntoContext into() {
			return getRuleContext(IntoContext.class,0);
		}
		public Upsert_pk_intoContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_upsert_pk_into; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterUpsert_pk_into(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitUpsert_pk_into(this);
		}
	}

	public final Upsert_pk_intoContext upsert_pk_into() throws RecognitionException {
		Upsert_pk_intoContext _localctx = new Upsert_pk_intoContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_upsert_pk_into);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(80);
			match(UPSERT);
			setState(81);
			pk();
			setState(82);
			match(ID);
			setState(83);
			into();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Sql_actionContext extends ParserRuleContext {
		public Insert_intoContext insert_into() {
			return getRuleContext(Insert_intoContext.class,0);
		}
		public Upsert_intoContext upsert_into() {
			return getRuleContext(Upsert_intoContext.class,0);
		}
		public Upsert_pk_intoContext upsert_pk_into() {
			return getRuleContext(Upsert_pk_intoContext.class,0);
		}
		public Sql_actionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sql_action; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterSql_action(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitSql_action(this);
		}
	}

	public final Sql_actionContext sql_action() throws RecognitionException {
		Sql_actionContext _localctx = new Sql_actionContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_sql_action);
		try {
			setState(88);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(85);
				insert_into();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(86);
				upsert_into();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(87);
				upsert_pk_into();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Schema_nameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public Schema_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_schema_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterSchema_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitSchema_name(this);
		}
	}

	public final Schema_nameContext schema_name() throws RecognitionException {
		Schema_nameContext _localctx = new Schema_nameContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_schema_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(90);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Select_clauseContext extends ParserRuleContext {
		public Sql_actionContext sql_action() {
			return getRuleContext(Sql_actionContext.class,0);
		}
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode SELECT() { return getToken(ConnectorParser.SELECT, 0); }
		public Column_listContext column_list() {
			return getRuleContext(Column_listContext.class,0);
		}
		public TerminalNode FROM() { return getToken(ConnectorParser.FROM, 0); }
		public Topic_nameContext topic_name() {
			return getRuleContext(Topic_nameContext.class,0);
		}
		public TerminalNode IGNORE() { return getToken(ConnectorParser.IGNORE, 0); }
		public Ignore_clauseContext ignore_clause() {
			return getRuleContext(Ignore_clauseContext.class,0);
		}
		public AutocreateContext autocreate() {
			return getRuleContext(AutocreateContext.class,0);
		}
		public TerminalNode PK() { return getToken(ConnectorParser.PK, 0); }
		public Primary_key_listContext primary_key_list() {
			return getRuleContext(Primary_key_listContext.class,0);
		}
		public AutoevolveContext autoevolve() {
			return getRuleContext(AutoevolveContext.class,0);
		}
		public BatchingContext batching() {
			return getRuleContext(BatchingContext.class,0);
		}
		public CapitalizeContext capitalize() {
			return getRuleContext(CapitalizeContext.class,0);
		}
		public PartitionbyContext partitionby() {
			return getRuleContext(PartitionbyContext.class,0);
		}
		public DistributebyContext distributeby() {
			return getRuleContext(DistributebyContext.class,0);
		}
		public ClusterbyContext clusterby() {
			return getRuleContext(ClusterbyContext.class,0);
		}
		public Select_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterSelect_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitSelect_clause(this);
		}
	}

	public final Select_clauseContext select_clause() throws RecognitionException {
		Select_clauseContext _localctx = new Select_clauseContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_select_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(92);
			sql_action();
			setState(93);
			table_name();
			setState(94);
			match(SELECT);
			setState(95);
			column_list();
			setState(96);
			match(FROM);
			setState(97);
			topic_name();
			setState(100);
			_la = _input.LA(1);
			if (_la==IGNORE) {
				{
				setState(98);
				match(IGNORE);
				setState(99);
				ignore_clause();
				}
			}

			setState(103);
			_la = _input.LA(1);
			if (_la==AUTOCREATE) {
				{
				setState(102);
				autocreate();
				}
			}

			setState(107);
			_la = _input.LA(1);
			if (_la==PK) {
				{
				setState(105);
				match(PK);
				setState(106);
				primary_key_list();
				}
			}

			setState(110);
			_la = _input.LA(1);
			if (_la==AUTOEVOLVE) {
				{
				setState(109);
				autoevolve();
				}
			}

			setState(113);
			_la = _input.LA(1);
			if (_la==BATCH) {
				{
				setState(112);
				batching();
				}
			}

			setState(116);
			_la = _input.LA(1);
			if (_la==CAPITALIZE) {
				{
				setState(115);
				capitalize();
				}
			}

			setState(119);
			_la = _input.LA(1);
			if (_la==PARTITIONBY) {
				{
				setState(118);
				partitionby();
				}
			}

			setState(122);
			_la = _input.LA(1);
			if (_la==DISTRIBUTEBY) {
				{
				setState(121);
				distributeby();
				}
			}

			setState(125);
			_la = _input.LA(1);
			if (_la==CLUSTERBY) {
				{
				setState(124);
				clusterby();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Topic_nameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public TerminalNode TOPICNAME() { return getToken(ConnectorParser.TOPICNAME, 0); }
		public Topic_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_topic_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterTopic_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitTopic_name(this);
		}
	}

	public final Topic_nameContext topic_name() throws RecognitionException {
		Topic_nameContext _localctx = new Topic_nameContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_topic_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(127);
			_la = _input.LA(1);
			if ( !(_la==ID || _la==TOPICNAME) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Table_nameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public TerminalNode TOPICNAME() { return getToken(ConnectorParser.TOPICNAME, 0); }
		public Table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterTable_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitTable_name(this);
		}
	}

	public final Table_nameContext table_name() throws RecognitionException {
		Table_nameContext _localctx = new Table_nameContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_table_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(129);
			_la = _input.LA(1);
			if ( !(_la==ID || _la==TOPICNAME) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_nameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public TerminalNode AS() { return getToken(ConnectorParser.AS, 0); }
		public Column_name_aliasContext column_name_alias() {
			return getRuleContext(Column_name_aliasContext.class,0);
		}
		public TerminalNode ASTERISK() { return getToken(ConnectorParser.ASTERISK, 0); }
		public Column_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterColumn_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitColumn_name(this);
		}
	}

	public final Column_nameContext column_name() throws RecognitionException {
		Column_nameContext _localctx = new Column_nameContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_column_name);
		int _la;
		try {
			setState(137);
			switch (_input.LA(1)) {
			case ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(131);
				match(ID);
				setState(134);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(132);
					match(AS);
					setState(133);
					column_name_alias();
					}
				}

				}
				break;
			case ASTERISK:
				enterOuterAlt(_localctx, 2);
				{
				setState(136);
				match(ASTERISK);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_name_aliasContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public Column_name_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_name_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterColumn_name_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitColumn_name_alias(this);
		}
	}

	public final Column_name_aliasContext column_name_alias() throws RecognitionException {
		Column_name_aliasContext _localctx = new Column_name_aliasContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_column_name_alias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(139);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Column_listContext extends ParserRuleContext {
		public List<Column_nameContext> column_name() {
			return getRuleContexts(Column_nameContext.class);
		}
		public Column_nameContext column_name(int i) {
			return getRuleContext(Column_nameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(ConnectorParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(ConnectorParser.COMMA, i);
		}
		public Column_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterColumn_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitColumn_list(this);
		}
	}

	public final Column_listContext column_list() throws RecognitionException {
		Column_listContext _localctx = new Column_listContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_column_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(141);
			column_name();
			setState(146);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(142);
				match(COMMA);
				setState(143);
				column_name();
				}
				}
				setState(148);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class From_clauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(ConnectorParser.FROM, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public From_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_from_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterFrom_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitFrom_clause(this);
		}
	}

	public final From_clauseContext from_clause() throws RecognitionException {
		From_clauseContext _localctx = new From_clauseContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_from_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(149);
			match(FROM);
			setState(150);
			table_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Ignored_nameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public Ignored_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ignored_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterIgnored_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitIgnored_name(this);
		}
	}

	public final Ignored_nameContext ignored_name() throws RecognitionException {
		Ignored_nameContext _localctx = new Ignored_nameContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_ignored_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(152);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Ignore_clauseContext extends ParserRuleContext {
		public List<Ignored_nameContext> ignored_name() {
			return getRuleContexts(Ignored_nameContext.class);
		}
		public Ignored_nameContext ignored_name(int i) {
			return getRuleContext(Ignored_nameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(ConnectorParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(ConnectorParser.COMMA, i);
		}
		public Ignore_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ignore_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterIgnore_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitIgnore_clause(this);
		}
	}

	public final Ignore_clauseContext ignore_clause() throws RecognitionException {
		Ignore_clauseContext _localctx = new Ignore_clauseContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_ignore_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(154);
			ignored_name();
			setState(159);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(155);
				match(COMMA);
				setState(156);
				ignored_name();
				}
				}
				setState(161);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Pk_nameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public Pk_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pk_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterPk_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitPk_name(this);
		}
	}

	public final Pk_nameContext pk_name() throws RecognitionException {
		Pk_nameContext _localctx = new Pk_nameContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_pk_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(162);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Primary_key_listContext extends ParserRuleContext {
		public List<Pk_nameContext> pk_name() {
			return getRuleContexts(Pk_nameContext.class);
		}
		public Pk_nameContext pk_name(int i) {
			return getRuleContext(Pk_nameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(ConnectorParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(ConnectorParser.COMMA, i);
		}
		public Primary_key_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primary_key_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterPrimary_key_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitPrimary_key_list(this);
		}
	}

	public final Primary_key_listContext primary_key_list() throws RecognitionException {
		Primary_key_listContext _localctx = new Primary_key_listContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_primary_key_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(164);
			pk_name();
			setState(169);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(165);
				match(COMMA);
				setState(166);
				pk_name();
				}
				}
				setState(171);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AutocreateContext extends ParserRuleContext {
		public TerminalNode AUTOCREATE() { return getToken(ConnectorParser.AUTOCREATE, 0); }
		public AutocreateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_autocreate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterAutocreate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitAutocreate(this);
		}
	}

	public final AutocreateContext autocreate() throws RecognitionException {
		AutocreateContext _localctx = new AutocreateContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_autocreate);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(172);
			match(AUTOCREATE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AutoevolveContext extends ParserRuleContext {
		public TerminalNode AUTOEVOLVE() { return getToken(ConnectorParser.AUTOEVOLVE, 0); }
		public AutoevolveContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_autoevolve; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterAutoevolve(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitAutoevolve(this);
		}
	}

	public final AutoevolveContext autoevolve() throws RecognitionException {
		AutoevolveContext _localctx = new AutoevolveContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_autoevolve);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(174);
			match(AUTOEVOLVE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Batch_sizeContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public Batch_sizeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_batch_size; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterBatch_size(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitBatch_size(this);
		}
	}

	public final Batch_sizeContext batch_size() throws RecognitionException {
		Batch_sizeContext _localctx = new Batch_sizeContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_batch_size);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(176);
			match(INT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BatchingContext extends ParserRuleContext {
		public TerminalNode BATCH() { return getToken(ConnectorParser.BATCH, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public Batch_sizeContext batch_size() {
			return getRuleContext(Batch_sizeContext.class,0);
		}
		public BatchingContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_batching; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterBatching(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitBatching(this);
		}
	}

	public final BatchingContext batching() throws RecognitionException {
		BatchingContext _localctx = new BatchingContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_batching);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(178);
			match(BATCH);
			setState(179);
			match(EQUAL);
			setState(180);
			batch_size();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CapitalizeContext extends ParserRuleContext {
		public TerminalNode CAPITALIZE() { return getToken(ConnectorParser.CAPITALIZE, 0); }
		public CapitalizeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_capitalize; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterCapitalize(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitCapitalize(this);
		}
	}

	public final CapitalizeContext capitalize() throws RecognitionException {
		CapitalizeContext _localctx = new CapitalizeContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_capitalize);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(182);
			match(CAPITALIZE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Partition_nameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public Partition_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partition_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterPartition_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitPartition_name(this);
		}
	}

	public final Partition_nameContext partition_name() throws RecognitionException {
		Partition_nameContext _localctx = new Partition_nameContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_partition_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(184);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Partition_listContext extends ParserRuleContext {
		public List<Partition_nameContext> partition_name() {
			return getRuleContexts(Partition_nameContext.class);
		}
		public Partition_nameContext partition_name(int i) {
			return getRuleContext(Partition_nameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(ConnectorParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(ConnectorParser.COMMA, i);
		}
		public Partition_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partition_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterPartition_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitPartition_list(this);
		}
	}

	public final Partition_listContext partition_list() throws RecognitionException {
		Partition_listContext _localctx = new Partition_listContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_partition_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(186);
			partition_name();
			setState(191);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(187);
				match(COMMA);
				setState(188);
				partition_name();
				}
				}
				setState(193);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionbyContext extends ParserRuleContext {
		public TerminalNode PARTITIONBY() { return getToken(ConnectorParser.PARTITIONBY, 0); }
		public Partition_listContext partition_list() {
			return getRuleContext(Partition_listContext.class,0);
		}
		public PartitionbyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionby; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterPartitionby(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitPartitionby(this);
		}
	}

	public final PartitionbyContext partitionby() throws RecognitionException {
		PartitionbyContext _localctx = new PartitionbyContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_partitionby);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(194);
			match(PARTITIONBY);
			setState(195);
			partition_list();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Distribute_nameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public Distribute_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_distribute_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterDistribute_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitDistribute_name(this);
		}
	}

	public final Distribute_nameContext distribute_name() throws RecognitionException {
		Distribute_nameContext _localctx = new Distribute_nameContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_distribute_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(197);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Distribute_listContext extends ParserRuleContext {
		public List<Distribute_nameContext> distribute_name() {
			return getRuleContexts(Distribute_nameContext.class);
		}
		public Distribute_nameContext distribute_name(int i) {
			return getRuleContext(Distribute_nameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(ConnectorParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(ConnectorParser.COMMA, i);
		}
		public Distribute_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_distribute_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterDistribute_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitDistribute_list(this);
		}
	}

	public final Distribute_listContext distribute_list() throws RecognitionException {
		Distribute_listContext _localctx = new Distribute_listContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_distribute_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(199);
			distribute_name();
			setState(204);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(200);
				match(COMMA);
				setState(201);
				distribute_name();
				}
				}
				setState(206);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DistributebyContext extends ParserRuleContext {
		public TerminalNode DISTRIBUTEBY() { return getToken(ConnectorParser.DISTRIBUTEBY, 0); }
		public Distribute_listContext distribute_list() {
			return getRuleContext(Distribute_listContext.class,0);
		}
		public TerminalNode INTO() { return getToken(ConnectorParser.INTO, 0); }
		public Buckets_numberContext buckets_number() {
			return getRuleContext(Buckets_numberContext.class,0);
		}
		public TerminalNode BUCKETS() { return getToken(ConnectorParser.BUCKETS, 0); }
		public DistributebyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_distributeby; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterDistributeby(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitDistributeby(this);
		}
	}

	public final DistributebyContext distributeby() throws RecognitionException {
		DistributebyContext _localctx = new DistributebyContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_distributeby);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(207);
			match(DISTRIBUTEBY);
			setState(208);
			distribute_list();
			setState(209);
			match(INTO);
			setState(210);
			buckets_number();
			setState(211);
			match(BUCKETS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Buckets_numberContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public Buckets_numberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_buckets_number; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterBuckets_number(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitBuckets_number(this);
		}
	}

	public final Buckets_numberContext buckets_number() throws RecognitionException {
		Buckets_numberContext _localctx = new Buckets_numberContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_buckets_number);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(213);
			match(INT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Clusterby_nameContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public Clusterby_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_clusterby_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterClusterby_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitClusterby_name(this);
		}
	}

	public final Clusterby_nameContext clusterby_name() throws RecognitionException {
		Clusterby_nameContext _localctx = new Clusterby_nameContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_clusterby_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(215);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class Clusterby_listContext extends ParserRuleContext {
		public List<Clusterby_nameContext> clusterby_name() {
			return getRuleContexts(Clusterby_nameContext.class);
		}
		public Clusterby_nameContext clusterby_name(int i) {
			return getRuleContext(Clusterby_nameContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(ConnectorParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(ConnectorParser.COMMA, i);
		}
		public Clusterby_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_clusterby_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterClusterby_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitClusterby_list(this);
		}
	}

	public final Clusterby_listContext clusterby_list() throws RecognitionException {
		Clusterby_listContext _localctx = new Clusterby_listContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_clusterby_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(217);
			clusterby_name();
			setState(222);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(218);
				match(COMMA);
				setState(219);
				clusterby_name();
				}
				}
				setState(224);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ClusterbyContext extends ParserRuleContext {
		public TerminalNode CLUSTERBY() { return getToken(ConnectorParser.CLUSTERBY, 0); }
		public Clusterby_listContext clusterby_list() {
			return getRuleContext(Clusterby_listContext.class,0);
		}
		public TerminalNode INTO() { return getToken(ConnectorParser.INTO, 0); }
		public Buckets_numberContext buckets_number() {
			return getRuleContext(Buckets_numberContext.class,0);
		}
		public TerminalNode BUCKETS() { return getToken(ConnectorParser.BUCKETS, 0); }
		public ClusterbyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_clusterby; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterClusterby(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitClusterby(this);
		}
	}

	public final ClusterbyContext clusterby() throws RecognitionException {
		ClusterbyContext _localctx = new ClusterbyContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_clusterby);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(225);
			match(CLUSTERBY);
			setState(226);
			clusterby_list();
			setState(227);
			match(INTO);
			setState(228);
			buckets_number();
			setState(229);
			match(BUCKETS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\33\u00ea\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3\6\3\7"+
		"\3\7\3\7\3\7\3\7\3\b\3\b\3\b\5\b[\n\b\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\5\ng\n\n\3\n\5\nj\n\n\3\n\3\n\5\nn\n\n\3\n\5\nq\n\n\3\n\5\nt"+
		"\n\n\3\n\5\nw\n\n\3\n\5\nz\n\n\3\n\5\n}\n\n\3\n\5\n\u0080\n\n\3\13\3\13"+
		"\3\f\3\f\3\r\3\r\3\r\5\r\u0089\n\r\3\r\5\r\u008c\n\r\3\16\3\16\3\17\3"+
		"\17\3\17\7\17\u0093\n\17\f\17\16\17\u0096\13\17\3\20\3\20\3\20\3\21\3"+
		"\21\3\22\3\22\3\22\7\22\u00a0\n\22\f\22\16\22\u00a3\13\22\3\23\3\23\3"+
		"\24\3\24\3\24\7\24\u00aa\n\24\f\24\16\24\u00ad\13\24\3\25\3\25\3\26\3"+
		"\26\3\27\3\27\3\30\3\30\3\30\3\30\3\31\3\31\3\32\3\32\3\33\3\33\3\33\7"+
		"\33\u00c0\n\33\f\33\16\33\u00c3\13\33\3\34\3\34\3\34\3\35\3\35\3\36\3"+
		"\36\3\36\7\36\u00cd\n\36\f\36\16\36\u00d0\13\36\3\37\3\37\3\37\3\37\3"+
		"\37\3\37\3 \3 \3!\3!\3\"\3\"\3\"\7\"\u00df\n\"\f\"\16\"\u00e2\13\"\3#"+
		"\3#\3#\3#\3#\3#\3#\2\2$\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*"+
		",.\60\62\64\668:<>@BD\2\3\3\2\27\30\u00da\2F\3\2\2\2\4H\3\2\2\2\6J\3\2"+
		"\2\2\bL\3\2\2\2\nO\3\2\2\2\fR\3\2\2\2\16Z\3\2\2\2\20\\\3\2\2\2\22^\3\2"+
		"\2\2\24\u0081\3\2\2\2\26\u0083\3\2\2\2\30\u008b\3\2\2\2\32\u008d\3\2\2"+
		"\2\34\u008f\3\2\2\2\36\u0097\3\2\2\2 \u009a\3\2\2\2\"\u009c\3\2\2\2$\u00a4"+
		"\3\2\2\2&\u00a6\3\2\2\2(\u00ae\3\2\2\2*\u00b0\3\2\2\2,\u00b2\3\2\2\2."+
		"\u00b4\3\2\2\2\60\u00b8\3\2\2\2\62\u00ba\3\2\2\2\64\u00bc\3\2\2\2\66\u00c4"+
		"\3\2\2\28\u00c7\3\2\2\2:\u00c9\3\2\2\2<\u00d1\3\2\2\2>\u00d7\3\2\2\2@"+
		"\u00d9\3\2\2\2B\u00db\3\2\2\2D\u00e3\3\2\2\2FG\5\22\n\2G\3\3\2\2\2HI\7"+
		"\5\2\2I\5\3\2\2\2JK\7\22\2\2K\7\3\2\2\2LM\7\3\2\2MN\5\4\3\2N\t\3\2\2\2"+
		"OP\7\4\2\2PQ\5\4\3\2Q\13\3\2\2\2RS\7\4\2\2ST\5\6\4\2TU\7\27\2\2UV\5\4"+
		"\3\2V\r\3\2\2\2W[\5\b\5\2X[\5\n\6\2Y[\5\f\7\2ZW\3\2\2\2ZX\3\2\2\2ZY\3"+
		"\2\2\2[\17\3\2\2\2\\]\7\27\2\2]\21\3\2\2\2^_\5\16\b\2_`\5\26\f\2`a\7\6"+
		"\2\2ab\5\34\17\2bc\7\7\2\2cf\5\24\13\2de\7\b\2\2eg\5\"\22\2fd\3\2\2\2"+
		"fg\3\2\2\2gi\3\2\2\2hj\5(\25\2ih\3\2\2\2ij\3\2\2\2jm\3\2\2\2kl\7\22\2"+
		"\2ln\5&\24\2mk\3\2\2\2mn\3\2\2\2np\3\2\2\2oq\5*\26\2po\3\2\2\2pq\3\2\2"+
		"\2qs\3\2\2\2rt\5.\30\2sr\3\2\2\2st\3\2\2\2tv\3\2\2\2uw\5\60\31\2vu\3\2"+
		"\2\2vw\3\2\2\2wy\3\2\2\2xz\5\66\34\2yx\3\2\2\2yz\3\2\2\2z|\3\2\2\2{}\5"+
		"<\37\2|{\3\2\2\2|}\3\2\2\2}\177\3\2\2\2~\u0080\5D#\2\177~\3\2\2\2\177"+
		"\u0080\3\2\2\2\u0080\23\3\2\2\2\u0081\u0082\t\2\2\2\u0082\25\3\2\2\2\u0083"+
		"\u0084\t\2\2\2\u0084\27\3\2\2\2\u0085\u0088\7\27\2\2\u0086\u0087\7\t\2"+
		"\2\u0087\u0089\5\32\16\2\u0088\u0086\3\2\2\2\u0088\u0089\3\2\2\2\u0089"+
		"\u008c\3\2\2\2\u008a\u008c\7\24\2\2\u008b\u0085\3\2\2\2\u008b\u008a\3"+
		"\2\2\2\u008c\31\3\2\2\2\u008d\u008e\7\27\2\2\u008e\33\3\2\2\2\u008f\u0094"+
		"\5\30\r\2\u0090\u0091\7\25\2\2\u0091\u0093\5\30\r\2\u0092\u0090\3\2\2"+
		"\2\u0093\u0096\3\2\2\2\u0094\u0092\3\2\2\2\u0094\u0095\3\2\2\2\u0095\35"+
		"\3\2\2\2\u0096\u0094\3\2\2\2\u0097\u0098\7\7\2\2\u0098\u0099\5\26\f\2"+
		"\u0099\37\3\2\2\2\u009a\u009b\7\27\2\2\u009b!\3\2\2\2\u009c\u00a1\5 \21"+
		"\2\u009d\u009e\7\25\2\2\u009e\u00a0\5 \21\2\u009f\u009d\3\2\2\2\u00a0"+
		"\u00a3\3\2\2\2\u00a1\u009f\3\2\2\2\u00a1\u00a2\3\2\2\2\u00a2#\3\2\2\2"+
		"\u00a3\u00a1\3\2\2\2\u00a4\u00a5\7\27\2\2\u00a5%\3\2\2\2\u00a6\u00ab\5"+
		"$\23\2\u00a7\u00a8\7\25\2\2\u00a8\u00aa\5$\23\2\u00a9\u00a7\3\2\2\2\u00aa"+
		"\u00ad\3\2\2\2\u00ab\u00a9\3\2\2\2\u00ab\u00ac\3\2\2\2\u00ac\'\3\2\2\2"+
		"\u00ad\u00ab\3\2\2\2\u00ae\u00af\7\n\2\2\u00af)\3\2\2\2\u00b0\u00b1\7"+
		"\13\2\2\u00b1+\3\2\2\2\u00b2\u00b3\7\23\2\2\u00b3-\3\2\2\2\u00b4\u00b5"+
		"\7\16\2\2\u00b5\u00b6\7\33\2\2\u00b6\u00b7\5,\27\2\u00b7/\3\2\2\2\u00b8"+
		"\u00b9\7\17\2\2\u00b9\61\3\2\2\2\u00ba\u00bb\7\27\2\2\u00bb\63\3\2\2\2"+
		"\u00bc\u00c1\5\62\32\2\u00bd\u00be\7\25\2\2\u00be\u00c0\5\62\32\2\u00bf"+
		"\u00bd\3\2\2\2\u00c0\u00c3\3\2\2\2\u00c1\u00bf\3\2\2\2\u00c1\u00c2\3\2"+
		"\2\2\u00c2\65\3\2\2\2\u00c3\u00c1\3\2\2\2\u00c4\u00c5\7\20\2\2\u00c5\u00c6"+
		"\5\64\33\2\u00c6\67\3\2\2\2\u00c7\u00c8\7\27\2\2\u00c89\3\2\2\2\u00c9"+
		"\u00ce\58\35\2\u00ca\u00cb\7\25\2\2\u00cb\u00cd\58\35\2\u00cc\u00ca\3"+
		"\2\2\2\u00cd\u00d0\3\2\2\2\u00ce\u00cc\3\2\2\2\u00ce\u00cf\3\2\2\2\u00cf"+
		";\3\2\2\2\u00d0\u00ce\3\2\2\2\u00d1\u00d2\7\21\2\2\u00d2\u00d3\5:\36\2"+
		"\u00d3\u00d4\7\5\2\2\u00d4\u00d5\5> \2\u00d5\u00d6\7\r\2\2\u00d6=\3\2"+
		"\2\2\u00d7\u00d8\7\23\2\2\u00d8?\3\2\2\2\u00d9\u00da\7\27\2\2\u00daA\3"+
		"\2\2\2\u00db\u00e0\5@!\2\u00dc\u00dd\7\25\2\2\u00dd\u00df\5@!\2\u00de"+
		"\u00dc\3\2\2\2\u00df\u00e2\3\2\2\2\u00e0\u00de\3\2\2\2\u00e0\u00e1\3\2"+
		"\2\2\u00e1C\3\2\2\2\u00e2\u00e0\3\2\2\2\u00e3\u00e4\7\f\2\2\u00e4\u00e5"+
		"\5B\"\2\u00e5\u00e6\7\5\2\2\u00e6\u00e7\5> \2\u00e7\u00e8\7\r\2\2\u00e8"+
		"E\3\2\2\2\24Zfimpsvy|\177\u0088\u008b\u0094\u00a1\u00ab\u00c1\u00ce\u00e0";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}