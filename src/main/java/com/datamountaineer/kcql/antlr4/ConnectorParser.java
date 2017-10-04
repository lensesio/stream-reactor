// Generated from ConnectorParser.g4 by ANTLR 4.7
package com.datamountaineer.kcql.antlr4;
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
	static { RuntimeMetaData.checkVersion("4.7", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		INSERT=1, UPSERT=2, INTO=3, SELECT=4, FROM=5, IGNORE=6, AS=7, AUTOCREATE=8, 
		AUTOEVOLVE=9, CLUSTERBY=10, BUCKETS=11, BATCH=12, CAPITALIZE=13, INITIALIZE=14, 
		PARTITIONBY=15, DISTRIBUTEBY=16, TIMESTAMP=17, SYS_TIME=18, WITHGROUP=19, 
		WITHOFFSET=20, WITHTAG=21, WITHSTRUCTURE=22, WITHTYPE=23, PK=24, SAMPLE=25, 
		EVERY=26, WITHFORMAT=27, WITHUNWRAP=28, FORMAT=29, PROJECTTO=30, STOREAS=31, 
		LIMIT=32, INCREMENTALMODE=33, WITHDOCTYPE=34, WITHINDEXSUFFIX=35, WITHCONVERTER=36, 
		WITHJMSSELECTOR=37, TTL=38, EQUAL=39, INT=40, ASTERISK=41, COMMA=42, DOT=43, 
		LEFT_PARAN=44, RIGHT_PARAN=45, FIELD=46, TOPICNAME=47, NEWLINE=48, WS=49, 
		ID=50;
	public static final int
		RULE_stat = 0, RULE_into = 1, RULE_pk = 2, RULE_insert_into = 3, RULE_upsert_into = 4, 
		RULE_upsert_pk_into = 5, RULE_write_mode = 6, RULE_schema_name = 7, RULE_insert_from_clause = 8, 
		RULE_select_clause = 9, RULE_select_clause_basic = 10, RULE_topic_name = 11, 
		RULE_table_name = 12, RULE_column_name = 13, RULE_column = 14, RULE_column_name_alias = 15, 
		RULE_column_list = 16, RULE_from_clause = 17, RULE_ignored_name = 18, 
		RULE_with_ignore = 19, RULE_ignore_clause = 20, RULE_pk_name = 21, RULE_primary_key_list = 22, 
		RULE_autocreate = 23, RULE_autoevolve = 24, RULE_batch_size = 25, RULE_batching = 26, 
		RULE_capitalize = 27, RULE_initialize = 28, RULE_partition_name = 29, 
		RULE_partition_list = 30, RULE_partitionby = 31, RULE_distribute_name = 32, 
		RULE_distribute_list = 33, RULE_distributeby = 34, RULE_timestamp_clause = 35, 
		RULE_timestamp_value = 36, RULE_buckets_number = 37, RULE_clusterby_name = 38, 
		RULE_clusterby_list = 39, RULE_clusterby = 40, RULE_with_consumer_group = 41, 
		RULE_with_consumer_group_value = 42, RULE_offset_partition_inner = 43, 
		RULE_offset_partition = 44, RULE_partition_offset_list = 45, RULE_with_offset_list = 46, 
		RULE_limit_clause = 47, RULE_limit_value = 48, RULE_sample_clause = 49, 
		RULE_sample_value = 50, RULE_sample_period = 51, RULE_with_unwrap_clause = 52, 
		RULE_with_format_clause = 53, RULE_with_structure = 54, RULE_with_format = 55, 
		RULE_project_to = 56, RULE_version_number = 57, RULE_storeas_clause = 58, 
		RULE_storeas_type = 59, RULE_storeas_parameters = 60, RULE_storeas_parameters_tuple = 61, 
		RULE_storeas_parameter = 62, RULE_storeas_value = 63, RULE_with_tags = 64, 
		RULE_with_inc_mode = 65, RULE_inc_mode = 66, RULE_with_type = 67, RULE_with_type_value = 68, 
		RULE_with_doc_type = 69, RULE_doc_type = 70, RULE_with_index_suffix = 71, 
		RULE_index_suffix = 72, RULE_with_converter = 73, RULE_with_converter_value = 74, 
		RULE_with_jms_selector = 75, RULE_jms_selector_value = 76, RULE_tag_definition = 77, 
		RULE_tag_key = 78, RULE_tag_value = 79, RULE_ttl_clause = 80, RULE_ttl_type = 81;
	public static final String[] ruleNames = {
		"stat", "into", "pk", "insert_into", "upsert_into", "upsert_pk_into", 
		"write_mode", "schema_name", "insert_from_clause", "select_clause", "select_clause_basic", 
		"topic_name", "table_name", "column_name", "column", "column_name_alias", 
		"column_list", "from_clause", "ignored_name", "with_ignore", "ignore_clause", 
		"pk_name", "primary_key_list", "autocreate", "autoevolve", "batch_size", 
		"batching", "capitalize", "initialize", "partition_name", "partition_list", 
		"partitionby", "distribute_name", "distribute_list", "distributeby", "timestamp_clause", 
		"timestamp_value", "buckets_number", "clusterby_name", "clusterby_list", 
		"clusterby", "with_consumer_group", "with_consumer_group_value", "offset_partition_inner", 
		"offset_partition", "partition_offset_list", "with_offset_list", "limit_clause", 
		"limit_value", "sample_clause", "sample_value", "sample_period", "with_unwrap_clause", 
		"with_format_clause", "with_structure", "with_format", "project_to", "version_number", 
		"storeas_clause", "storeas_type", "storeas_parameters", "storeas_parameters_tuple", 
		"storeas_parameter", "storeas_value", "with_tags", "with_inc_mode", "inc_mode", 
		"with_type", "with_type_value", "with_doc_type", "doc_type", "with_index_suffix", 
		"index_suffix", "with_converter", "with_converter_value", "with_jms_selector", 
		"jms_selector_value", "tag_definition", "tag_key", "tag_value", "ttl_clause", 
		"ttl_type"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, "'='", null, "'*'", "','", "'.'", "'('", "')'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "INITIALIZE", 
		"PARTITIONBY", "DISTRIBUTEBY", "TIMESTAMP", "SYS_TIME", "WITHGROUP", "WITHOFFSET", 
		"WITHTAG", "WITHSTRUCTURE", "WITHTYPE", "PK", "SAMPLE", "EVERY", "WITHFORMAT", 
		"WITHUNWRAP", "FORMAT", "PROJECTTO", "STOREAS", "LIMIT", "INCREMENTALMODE", 
		"WITHDOCTYPE", "WITHINDEXSUFFIX", "WITHCONVERTER", "WITHJMSSELECTOR", 
		"TTL", "EQUAL", "INT", "ASTERISK", "COMMA", "DOT", "LEFT_PARAN", "RIGHT_PARAN", 
		"FIELD", "TOPICNAME", "NEWLINE", "WS", "ID"
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
		public Insert_from_clauseContext insert_from_clause() {
			return getRuleContext(Insert_from_clauseContext.class,0);
		}
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
			setState(166);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INSERT:
			case UPSERT:
				enterOuterAlt(_localctx, 1);
				{
				setState(164);
				insert_from_clause();
				}
				break;
			case SELECT:
				enterOuterAlt(_localctx, 2);
				{
				setState(165);
				select_clause();
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
			setState(168);
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
			setState(170);
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
			setState(172);
			match(INSERT);
			setState(173);
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
			setState(175);
			match(UPSERT);
			setState(176);
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
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
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
			{
			setState(178);
			match(UPSERT);
			setState(179);
			pk();
			setState(180);
			match(FIELD);
			setState(181);
			into();
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

	public static class Write_modeContext extends ParserRuleContext {
		public Insert_intoContext insert_into() {
			return getRuleContext(Insert_intoContext.class,0);
		}
		public Upsert_intoContext upsert_into() {
			return getRuleContext(Upsert_intoContext.class,0);
		}
		public Upsert_pk_intoContext upsert_pk_into() {
			return getRuleContext(Upsert_pk_intoContext.class,0);
		}
		public Write_modeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_write_mode; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWrite_mode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWrite_mode(this);
		}
	}

	public final Write_modeContext write_mode() throws RecognitionException {
		Write_modeContext _localctx = new Write_modeContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_write_mode);
		try {
			setState(186);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(183);
				insert_into();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(184);
				upsert_into();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(185);
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
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
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
			setState(188);
			match(FIELD);
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

	public static class Insert_from_clauseContext extends ParserRuleContext {
		public Write_modeContext write_mode() {
			return getRuleContext(Write_modeContext.class,0);
		}
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Select_clause_basicContext select_clause_basic() {
			return getRuleContext(Select_clause_basicContext.class,0);
		}
		public AutocreateContext autocreate() {
			return getRuleContext(AutocreateContext.class,0);
		}
		public With_structureContext with_structure() {
			return getRuleContext(With_structureContext.class,0);
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
		public InitializeContext initialize() {
			return getRuleContext(InitializeContext.class,0);
		}
		public Project_toContext project_to() {
			return getRuleContext(Project_toContext.class,0);
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
		public Timestamp_clauseContext timestamp_clause() {
			return getRuleContext(Timestamp_clauseContext.class,0);
		}
		public With_format_clauseContext with_format_clause() {
			return getRuleContext(With_format_clauseContext.class,0);
		}
		public With_unwrap_clauseContext with_unwrap_clause() {
			return getRuleContext(With_unwrap_clauseContext.class,0);
		}
		public Storeas_clauseContext storeas_clause() {
			return getRuleContext(Storeas_clauseContext.class,0);
		}
		public With_tagsContext with_tags() {
			return getRuleContext(With_tagsContext.class,0);
		}
		public With_inc_modeContext with_inc_mode() {
			return getRuleContext(With_inc_modeContext.class,0);
		}
		public With_typeContext with_type() {
			return getRuleContext(With_typeContext.class,0);
		}
		public With_doc_typeContext with_doc_type() {
			return getRuleContext(With_doc_typeContext.class,0);
		}
		public With_index_suffixContext with_index_suffix() {
			return getRuleContext(With_index_suffixContext.class,0);
		}
		public Ttl_clauseContext ttl_clause() {
			return getRuleContext(Ttl_clauseContext.class,0);
		}
		public With_converterContext with_converter() {
			return getRuleContext(With_converterContext.class,0);
		}
		public With_jms_selectorContext with_jms_selector() {
			return getRuleContext(With_jms_selectorContext.class,0);
		}
		public Insert_from_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert_from_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterInsert_from_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitInsert_from_clause(this);
		}
	}

	public final Insert_from_clauseContext insert_from_clause() throws RecognitionException {
		Insert_from_clauseContext _localctx = new Insert_from_clauseContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_insert_from_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(190);
			write_mode();
			setState(191);
			table_name();
			setState(192);
			select_clause_basic();
			setState(194);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AUTOCREATE) {
				{
				setState(193);
				autocreate();
				}
			}

			setState(197);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHSTRUCTURE) {
				{
				setState(196);
				with_structure();
				}
			}

			setState(201);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PK) {
				{
				setState(199);
				match(PK);
				setState(200);
				primary_key_list();
				}
			}

			setState(204);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AUTOEVOLVE) {
				{
				setState(203);
				autoevolve();
				}
			}

			setState(207);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==BATCH) {
				{
				setState(206);
				batching();
				}
			}

			setState(210);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CAPITALIZE) {
				{
				setState(209);
				capitalize();
				}
			}

			setState(213);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INITIALIZE) {
				{
				setState(212);
				initialize();
				}
			}

			setState(216);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PROJECTTO) {
				{
				setState(215);
				project_to();
				}
			}

			setState(219);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PARTITIONBY) {
				{
				setState(218);
				partitionby();
				}
			}

			setState(222);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DISTRIBUTEBY) {
				{
				setState(221);
				distributeby();
				}
			}

			setState(225);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CLUSTERBY) {
				{
				setState(224);
				clusterby();
				}
			}

			setState(228);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TIMESTAMP) {
				{
				setState(227);
				timestamp_clause();
				}
			}

			setState(231);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHFORMAT) {
				{
				setState(230);
				with_format_clause();
				}
			}

			setState(234);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHUNWRAP) {
				{
				setState(233);
				with_unwrap_clause();
				}
			}

			setState(237);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STOREAS) {
				{
				setState(236);
				storeas_clause();
				}
			}

			setState(240);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHTAG) {
				{
				setState(239);
				with_tags();
				}
			}

			setState(243);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INCREMENTALMODE) {
				{
				setState(242);
				with_inc_mode();
				}
			}

			setState(246);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHTYPE) {
				{
				setState(245);
				with_type();
				}
			}

			setState(249);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHDOCTYPE) {
				{
				setState(248);
				with_doc_type();
				}
			}

			setState(252);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHINDEXSUFFIX) {
				{
				setState(251);
				with_index_suffix();
				}
			}

			setState(255);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TTL) {
				{
				setState(254);
				ttl_clause();
				}
			}

			setState(258);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHCONVERTER) {
				{
				setState(257);
				with_converter();
				}
			}

			setState(261);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHJMSSELECTOR) {
				{
				setState(260);
				with_jms_selector();
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

	public static class Select_clauseContext extends ParserRuleContext {
		public Select_clause_basicContext select_clause_basic() {
			return getRuleContext(Select_clause_basicContext.class,0);
		}
		public TerminalNode PK() { return getToken(ConnectorParser.PK, 0); }
		public Primary_key_listContext primary_key_list() {
			return getRuleContext(Primary_key_listContext.class,0);
		}
		public With_structureContext with_structure() {
			return getRuleContext(With_structureContext.class,0);
		}
		public With_format_clauseContext with_format_clause() {
			return getRuleContext(With_format_clauseContext.class,0);
		}
		public With_unwrap_clauseContext with_unwrap_clause() {
			return getRuleContext(With_unwrap_clauseContext.class,0);
		}
		public With_consumer_groupContext with_consumer_group() {
			return getRuleContext(With_consumer_groupContext.class,0);
		}
		public With_offset_listContext with_offset_list() {
			return getRuleContext(With_offset_listContext.class,0);
		}
		public Sample_clauseContext sample_clause() {
			return getRuleContext(Sample_clauseContext.class,0);
		}
		public Limit_clauseContext limit_clause() {
			return getRuleContext(Limit_clauseContext.class,0);
		}
		public Storeas_clauseContext storeas_clause() {
			return getRuleContext(Storeas_clauseContext.class,0);
		}
		public With_tagsContext with_tags() {
			return getRuleContext(With_tagsContext.class,0);
		}
		public With_inc_modeContext with_inc_mode() {
			return getRuleContext(With_inc_modeContext.class,0);
		}
		public With_doc_typeContext with_doc_type() {
			return getRuleContext(With_doc_typeContext.class,0);
		}
		public With_index_suffixContext with_index_suffix() {
			return getRuleContext(With_index_suffixContext.class,0);
		}
		public With_converterContext with_converter() {
			return getRuleContext(With_converterContext.class,0);
		}
		public With_jms_selectorContext with_jms_selector() {
			return getRuleContext(With_jms_selectorContext.class,0);
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
		enterRule(_localctx, 18, RULE_select_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(263);
			select_clause_basic();
			setState(266);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PK) {
				{
				setState(264);
				match(PK);
				setState(265);
				primary_key_list();
				}
			}

			setState(269);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHSTRUCTURE) {
				{
				setState(268);
				with_structure();
				}
			}

			setState(272);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHFORMAT) {
				{
				setState(271);
				with_format_clause();
				}
			}

			setState(275);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHUNWRAP) {
				{
				setState(274);
				with_unwrap_clause();
				}
			}

			setState(278);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHGROUP) {
				{
				setState(277);
				with_consumer_group();
				}
			}

			setState(281);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHOFFSET) {
				{
				setState(280);
				with_offset_list();
				}
			}

			setState(284);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SAMPLE) {
				{
				setState(283);
				sample_clause();
				}
			}

			setState(287);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(286);
				limit_clause();
				}
			}

			setState(290);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STOREAS) {
				{
				setState(289);
				storeas_clause();
				}
			}

			setState(293);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHTAG) {
				{
				setState(292);
				with_tags();
				}
			}

			setState(296);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INCREMENTALMODE) {
				{
				setState(295);
				with_inc_mode();
				}
			}

			setState(299);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHDOCTYPE) {
				{
				setState(298);
				with_doc_type();
				}
			}

			setState(302);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHINDEXSUFFIX) {
				{
				setState(301);
				with_index_suffix();
				}
			}

			setState(305);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHCONVERTER) {
				{
				setState(304);
				with_converter();
				}
			}

			setState(308);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHJMSSELECTOR) {
				{
				setState(307);
				with_jms_selector();
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

	public static class Select_clause_basicContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(ConnectorParser.SELECT, 0); }
		public Column_listContext column_list() {
			return getRuleContext(Column_listContext.class,0);
		}
		public TerminalNode FROM() { return getToken(ConnectorParser.FROM, 0); }
		public Topic_nameContext topic_name() {
			return getRuleContext(Topic_nameContext.class,0);
		}
		public With_ignoreContext with_ignore() {
			return getRuleContext(With_ignoreContext.class,0);
		}
		public Select_clause_basicContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_clause_basic; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterSelect_clause_basic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitSelect_clause_basic(this);
		}
	}

	public final Select_clause_basicContext select_clause_basic() throws RecognitionException {
		Select_clause_basicContext _localctx = new Select_clause_basicContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_select_clause_basic);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(310);
			match(SELECT);
			setState(311);
			column_list();
			setState(312);
			match(FROM);
			setState(313);
			topic_name();
			setState(315);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==IGNORE) {
				{
				setState(314);
				with_ignore();
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
		public List<TerminalNode> FIELD() { return getTokens(ConnectorParser.FIELD); }
		public TerminalNode FIELD(int i) {
			return getToken(ConnectorParser.FIELD, i);
		}
		public List<TerminalNode> TOPICNAME() { return getTokens(ConnectorParser.TOPICNAME); }
		public TerminalNode TOPICNAME(int i) {
			return getToken(ConnectorParser.TOPICNAME, i);
		}
		public List<TerminalNode> DOT() { return getTokens(ConnectorParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(ConnectorParser.DOT, i);
		}
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
		enterRule(_localctx, 22, RULE_topic_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(318); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(317);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << DOT) | (1L << FIELD) | (1L << TOPICNAME))) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				}
				setState(320); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << DOT) | (1L << FIELD) | (1L << TOPICNAME))) != 0) );
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
		public List<TerminalNode> FIELD() { return getTokens(ConnectorParser.FIELD); }
		public TerminalNode FIELD(int i) {
			return getToken(ConnectorParser.FIELD, i);
		}
		public List<TerminalNode> TOPICNAME() { return getTokens(ConnectorParser.TOPICNAME); }
		public TerminalNode TOPICNAME(int i) {
			return getToken(ConnectorParser.TOPICNAME, i);
		}
		public List<TerminalNode> DOT() { return getTokens(ConnectorParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(ConnectorParser.DOT, i);
		}
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
		enterRule(_localctx, 24, RULE_table_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(323); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(322);
				_la = _input.LA(1);
				if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << DOT) | (1L << FIELD) | (1L << TOPICNAME))) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				}
				setState(325); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << DOT) | (1L << FIELD) | (1L << TOPICNAME))) != 0) );
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
		public ColumnContext column() {
			return getRuleContext(ColumnContext.class,0);
		}
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
		enterRule(_localctx, 26, RULE_column_name);
		int _la;
		try {
			setState(333);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(327);
				column();
				setState(330);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(328);
					match(AS);
					setState(329);
					column_name_alias();
					}
				}

				}
				break;
			case ASTERISK:
				enterOuterAlt(_localctx, 2);
				{
				setState(332);
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

	public static class ColumnContext extends ParserRuleContext {
		public List<TerminalNode> FIELD() { return getTokens(ConnectorParser.FIELD); }
		public TerminalNode FIELD(int i) {
			return getToken(ConnectorParser.FIELD, i);
		}
		public List<TerminalNode> DOT() { return getTokens(ConnectorParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(ConnectorParser.DOT, i);
		}
		public TerminalNode ASTERISK() { return getToken(ConnectorParser.ASTERISK, 0); }
		public ColumnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterColumn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitColumn(this);
		}
	}

	public final ColumnContext column() throws RecognitionException {
		ColumnContext _localctx = new ColumnContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_column);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(335);
			match(FIELD);
			setState(340);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,45,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(336);
					match(DOT);
					setState(337);
					match(FIELD);
					}
					} 
				}
				setState(342);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,45,_ctx);
			}
			setState(345);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DOT) {
				{
				setState(343);
				match(DOT);
				setState(344);
				match(ASTERISK);
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

	public static class Column_name_aliasContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
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
		enterRule(_localctx, 30, RULE_column_name_alias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(347);
			match(FIELD);
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
		enterRule(_localctx, 32, RULE_column_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(349);
			column_name();
			setState(354);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(350);
				match(COMMA);
				setState(351);
				column_name();
				}
				}
				setState(356);
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
		enterRule(_localctx, 34, RULE_from_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(357);
			match(FROM);
			setState(358);
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
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public TerminalNode TOPICNAME() { return getToken(ConnectorParser.TOPICNAME, 0); }
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
		enterRule(_localctx, 36, RULE_ignored_name);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(360);
			_la = _input.LA(1);
			if ( !(_la==FIELD || _la==TOPICNAME) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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

	public static class With_ignoreContext extends ParserRuleContext {
		public TerminalNode IGNORE() { return getToken(ConnectorParser.IGNORE, 0); }
		public Ignore_clauseContext ignore_clause() {
			return getRuleContext(Ignore_clauseContext.class,0);
		}
		public With_ignoreContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_ignore; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_ignore(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_ignore(this);
		}
	}

	public final With_ignoreContext with_ignore() throws RecognitionException {
		With_ignoreContext _localctx = new With_ignoreContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_with_ignore);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(362);
			match(IGNORE);
			setState(363);
			ignore_clause();
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
		enterRule(_localctx, 40, RULE_ignore_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(365);
			column_name();
			setState(370);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(366);
				match(COMMA);
				setState(367);
				column_name();
				}
				}
				setState(372);
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
		public ColumnContext column() {
			return getRuleContext(ColumnContext.class,0);
		}
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
		enterRule(_localctx, 42, RULE_pk_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(373);
			column();
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
		enterRule(_localctx, 44, RULE_primary_key_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(375);
			pk_name();
			setState(380);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(376);
				match(COMMA);
				setState(377);
				pk_name();
				}
				}
				setState(382);
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
		enterRule(_localctx, 46, RULE_autocreate);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(383);
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
		enterRule(_localctx, 48, RULE_autoevolve);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(385);
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
		enterRule(_localctx, 50, RULE_batch_size);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(387);
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
		enterRule(_localctx, 52, RULE_batching);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(389);
			match(BATCH);
			setState(390);
			match(EQUAL);
			setState(391);
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
		enterRule(_localctx, 54, RULE_capitalize);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(393);
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

	public static class InitializeContext extends ParserRuleContext {
		public TerminalNode INITIALIZE() { return getToken(ConnectorParser.INITIALIZE, 0); }
		public InitializeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_initialize; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterInitialize(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitInitialize(this);
		}
	}

	public final InitializeContext initialize() throws RecognitionException {
		InitializeContext _localctx = new InitializeContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_initialize);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(395);
			match(INITIALIZE);
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
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
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
		enterRule(_localctx, 58, RULE_partition_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(397);
			match(FIELD);
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
		enterRule(_localctx, 60, RULE_partition_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(399);
			partition_name();
			setState(404);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(400);
				match(COMMA);
				setState(401);
				partition_name();
				}
				}
				setState(406);
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
		enterRule(_localctx, 62, RULE_partitionby);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(407);
			match(PARTITIONBY);
			setState(408);
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
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
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
		enterRule(_localctx, 64, RULE_distribute_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(410);
			match(FIELD);
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
		enterRule(_localctx, 66, RULE_distribute_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(412);
			distribute_name();
			setState(417);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(413);
				match(COMMA);
				setState(414);
				distribute_name();
				}
				}
				setState(419);
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
		enterRule(_localctx, 68, RULE_distributeby);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(420);
			match(DISTRIBUTEBY);
			setState(421);
			distribute_list();
			setState(422);
			match(INTO);
			setState(423);
			buckets_number();
			setState(424);
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

	public static class Timestamp_clauseContext extends ParserRuleContext {
		public TerminalNode TIMESTAMP() { return getToken(ConnectorParser.TIMESTAMP, 0); }
		public Timestamp_valueContext timestamp_value() {
			return getRuleContext(Timestamp_valueContext.class,0);
		}
		public Timestamp_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timestamp_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterTimestamp_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitTimestamp_clause(this);
		}
	}

	public final Timestamp_clauseContext timestamp_clause() throws RecognitionException {
		Timestamp_clauseContext _localctx = new Timestamp_clauseContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_timestamp_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(426);
			match(TIMESTAMP);
			setState(427);
			timestamp_value();
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

	public static class Timestamp_valueContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public TerminalNode SYS_TIME() { return getToken(ConnectorParser.SYS_TIME, 0); }
		public Timestamp_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timestamp_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterTimestamp_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitTimestamp_value(this);
		}
	}

	public final Timestamp_valueContext timestamp_value() throws RecognitionException {
		Timestamp_valueContext _localctx = new Timestamp_valueContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_timestamp_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(429);
			_la = _input.LA(1);
			if ( !(_la==SYS_TIME || _la==FIELD) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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
		enterRule(_localctx, 74, RULE_buckets_number);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(431);
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
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
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
		enterRule(_localctx, 76, RULE_clusterby_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(433);
			match(FIELD);
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
		enterRule(_localctx, 78, RULE_clusterby_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(435);
			clusterby_name();
			setState(440);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(436);
				match(COMMA);
				setState(437);
				clusterby_name();
				}
				}
				setState(442);
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
		enterRule(_localctx, 80, RULE_clusterby);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(443);
			match(CLUSTERBY);
			setState(444);
			clusterby_list();
			setState(445);
			match(INTO);
			setState(446);
			buckets_number();
			setState(447);
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

	public static class With_consumer_groupContext extends ParserRuleContext {
		public TerminalNode WITHGROUP() { return getToken(ConnectorParser.WITHGROUP, 0); }
		public With_consumer_group_valueContext with_consumer_group_value() {
			return getRuleContext(With_consumer_group_valueContext.class,0);
		}
		public With_consumer_groupContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_consumer_group; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_consumer_group(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_consumer_group(this);
		}
	}

	public final With_consumer_groupContext with_consumer_group() throws RecognitionException {
		With_consumer_groupContext _localctx = new With_consumer_groupContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_with_consumer_group);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(449);
			match(WITHGROUP);
			setState(450);
			with_consumer_group_value();
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

	public static class With_consumer_group_valueContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public TerminalNode TOPICNAME() { return getToken(ConnectorParser.TOPICNAME, 0); }
		public With_consumer_group_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_consumer_group_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_consumer_group_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_consumer_group_value(this);
		}
	}

	public final With_consumer_group_valueContext with_consumer_group_value() throws RecognitionException {
		With_consumer_group_valueContext _localctx = new With_consumer_group_valueContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_with_consumer_group_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(452);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << INT) | (1L << FIELD) | (1L << TOPICNAME))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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

	public static class Offset_partition_innerContext extends ParserRuleContext {
		public List<TerminalNode> INT() { return getTokens(ConnectorParser.INT); }
		public TerminalNode INT(int i) {
			return getToken(ConnectorParser.INT, i);
		}
		public TerminalNode COMMA() { return getToken(ConnectorParser.COMMA, 0); }
		public Offset_partition_innerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_offset_partition_inner; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterOffset_partition_inner(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitOffset_partition_inner(this);
		}
	}

	public final Offset_partition_innerContext offset_partition_inner() throws RecognitionException {
		Offset_partition_innerContext _localctx = new Offset_partition_innerContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_offset_partition_inner);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(454);
			match(INT);
			setState(457);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(455);
				match(COMMA);
				setState(456);
				match(INT);
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

	public static class Offset_partitionContext extends ParserRuleContext {
		public TerminalNode LEFT_PARAN() { return getToken(ConnectorParser.LEFT_PARAN, 0); }
		public Offset_partition_innerContext offset_partition_inner() {
			return getRuleContext(Offset_partition_innerContext.class,0);
		}
		public TerminalNode RIGHT_PARAN() { return getToken(ConnectorParser.RIGHT_PARAN, 0); }
		public Offset_partitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_offset_partition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterOffset_partition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitOffset_partition(this);
		}
	}

	public final Offset_partitionContext offset_partition() throws RecognitionException {
		Offset_partitionContext _localctx = new Offset_partitionContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_offset_partition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(459);
			match(LEFT_PARAN);
			setState(460);
			offset_partition_inner();
			setState(461);
			match(RIGHT_PARAN);
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

	public static class Partition_offset_listContext extends ParserRuleContext {
		public List<Offset_partitionContext> offset_partition() {
			return getRuleContexts(Offset_partitionContext.class);
		}
		public Offset_partitionContext offset_partition(int i) {
			return getRuleContext(Offset_partitionContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(ConnectorParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(ConnectorParser.COMMA, i);
		}
		public Partition_offset_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partition_offset_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterPartition_offset_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitPartition_offset_list(this);
		}
	}

	public final Partition_offset_listContext partition_offset_list() throws RecognitionException {
		Partition_offset_listContext _localctx = new Partition_offset_listContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_partition_offset_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(463);
			offset_partition();
			setState(468);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(464);
				match(COMMA);
				setState(465);
				offset_partition();
				}
				}
				setState(470);
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

	public static class With_offset_listContext extends ParserRuleContext {
		public TerminalNode WITHOFFSET() { return getToken(ConnectorParser.WITHOFFSET, 0); }
		public Partition_offset_listContext partition_offset_list() {
			return getRuleContext(Partition_offset_listContext.class,0);
		}
		public With_offset_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_offset_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_offset_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_offset_list(this);
		}
	}

	public final With_offset_listContext with_offset_list() throws RecognitionException {
		With_offset_listContext _localctx = new With_offset_listContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_with_offset_list);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(471);
			match(WITHOFFSET);
			setState(472);
			partition_offset_list();
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

	public static class Limit_clauseContext extends ParserRuleContext {
		public TerminalNode LIMIT() { return getToken(ConnectorParser.LIMIT, 0); }
		public Limit_valueContext limit_value() {
			return getRuleContext(Limit_valueContext.class,0);
		}
		public Limit_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limit_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterLimit_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitLimit_clause(this);
		}
	}

	public final Limit_clauseContext limit_clause() throws RecognitionException {
		Limit_clauseContext _localctx = new Limit_clauseContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_limit_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(474);
			match(LIMIT);
			setState(475);
			limit_value();
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

	public static class Limit_valueContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public Limit_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limit_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterLimit_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitLimit_value(this);
		}
	}

	public final Limit_valueContext limit_value() throws RecognitionException {
		Limit_valueContext _localctx = new Limit_valueContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_limit_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(477);
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

	public static class Sample_clauseContext extends ParserRuleContext {
		public TerminalNode SAMPLE() { return getToken(ConnectorParser.SAMPLE, 0); }
		public Sample_valueContext sample_value() {
			return getRuleContext(Sample_valueContext.class,0);
		}
		public TerminalNode EVERY() { return getToken(ConnectorParser.EVERY, 0); }
		public Sample_periodContext sample_period() {
			return getRuleContext(Sample_periodContext.class,0);
		}
		public Sample_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sample_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterSample_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitSample_clause(this);
		}
	}

	public final Sample_clauseContext sample_clause() throws RecognitionException {
		Sample_clauseContext _localctx = new Sample_clauseContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_sample_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(479);
			match(SAMPLE);
			setState(480);
			sample_value();
			setState(481);
			match(EVERY);
			setState(482);
			sample_period();
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

	public static class Sample_valueContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public Sample_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sample_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterSample_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitSample_value(this);
		}
	}

	public final Sample_valueContext sample_value() throws RecognitionException {
		Sample_valueContext _localctx = new Sample_valueContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_sample_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(484);
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

	public static class Sample_periodContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public Sample_periodContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sample_period; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterSample_period(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitSample_period(this);
		}
	}

	public final Sample_periodContext sample_period() throws RecognitionException {
		Sample_periodContext _localctx = new Sample_periodContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_sample_period);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(486);
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

	public static class With_unwrap_clauseContext extends ParserRuleContext {
		public TerminalNode WITHUNWRAP() { return getToken(ConnectorParser.WITHUNWRAP, 0); }
		public With_unwrap_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_unwrap_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_unwrap_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_unwrap_clause(this);
		}
	}

	public final With_unwrap_clauseContext with_unwrap_clause() throws RecognitionException {
		With_unwrap_clauseContext _localctx = new With_unwrap_clauseContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_with_unwrap_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(488);
			match(WITHUNWRAP);
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

	public static class With_format_clauseContext extends ParserRuleContext {
		public TerminalNode WITHFORMAT() { return getToken(ConnectorParser.WITHFORMAT, 0); }
		public With_formatContext with_format() {
			return getRuleContext(With_formatContext.class,0);
		}
		public With_format_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_format_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_format_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_format_clause(this);
		}
	}

	public final With_format_clauseContext with_format_clause() throws RecognitionException {
		With_format_clauseContext _localctx = new With_format_clauseContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_with_format_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(490);
			match(WITHFORMAT);
			setState(491);
			with_format();
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

	public static class With_structureContext extends ParserRuleContext {
		public TerminalNode WITHSTRUCTURE() { return getToken(ConnectorParser.WITHSTRUCTURE, 0); }
		public With_structureContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_structure; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_structure(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_structure(this);
		}
	}

	public final With_structureContext with_structure() throws RecognitionException {
		With_structureContext _localctx = new With_structureContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_with_structure);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(493);
			match(WITHSTRUCTURE);
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

	public static class With_formatContext extends ParserRuleContext {
		public TerminalNode FORMAT() { return getToken(ConnectorParser.FORMAT, 0); }
		public With_formatContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_format; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_format(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_format(this);
		}
	}

	public final With_formatContext with_format() throws RecognitionException {
		With_formatContext _localctx = new With_formatContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_with_format);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(495);
			match(FORMAT);
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

	public static class Project_toContext extends ParserRuleContext {
		public TerminalNode PROJECTTO() { return getToken(ConnectorParser.PROJECTTO, 0); }
		public Version_numberContext version_number() {
			return getRuleContext(Version_numberContext.class,0);
		}
		public Project_toContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_project_to; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterProject_to(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitProject_to(this);
		}
	}

	public final Project_toContext project_to() throws RecognitionException {
		Project_toContext _localctx = new Project_toContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_project_to);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(497);
			match(PROJECTTO);
			setState(498);
			version_number();
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

	public static class Version_numberContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public Version_numberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_version_number; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterVersion_number(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitVersion_number(this);
		}
	}

	public final Version_numberContext version_number() throws RecognitionException {
		Version_numberContext _localctx = new Version_numberContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_version_number);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(500);
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

	public static class Storeas_clauseContext extends ParserRuleContext {
		public TerminalNode STOREAS() { return getToken(ConnectorParser.STOREAS, 0); }
		public Storeas_typeContext storeas_type() {
			return getRuleContext(Storeas_typeContext.class,0);
		}
		public List<Storeas_parametersContext> storeas_parameters() {
			return getRuleContexts(Storeas_parametersContext.class);
		}
		public Storeas_parametersContext storeas_parameters(int i) {
			return getRuleContext(Storeas_parametersContext.class,i);
		}
		public Storeas_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storeas_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterStoreas_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitStoreas_clause(this);
		}
	}

	public final Storeas_clauseContext storeas_clause() throws RecognitionException {
		Storeas_clauseContext _localctx = new Storeas_clauseContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_storeas_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(502);
			match(STOREAS);
			setState(503);
			storeas_type();
			setState(507);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==LEFT_PARAN) {
				{
				{
				setState(504);
				storeas_parameters();
				}
				}
				setState(509);
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

	public static class Storeas_typeContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public List<TerminalNode> DOT() { return getTokens(ConnectorParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(ConnectorParser.DOT, i);
		}
		public List<TerminalNode> TOPICNAME() { return getTokens(ConnectorParser.TOPICNAME); }
		public TerminalNode TOPICNAME(int i) {
			return getToken(ConnectorParser.TOPICNAME, i);
		}
		public Storeas_typeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storeas_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterStoreas_type(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitStoreas_type(this);
		}
	}

	public final Storeas_typeContext storeas_type() throws RecognitionException {
		Storeas_typeContext _localctx = new Storeas_typeContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_storeas_type);
		int _la;
		try {
			setState(516);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(510);
				match(FIELD);
				}
				break;
			case DOT:
			case TOPICNAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(512); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(511);
					_la = _input.LA(1);
					if ( !(_la==DOT || _la==TOPICNAME) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(514); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==TOPICNAME );
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

	public static class Storeas_parametersContext extends ParserRuleContext {
		public TerminalNode LEFT_PARAN() { return getToken(ConnectorParser.LEFT_PARAN, 0); }
		public List<Storeas_parameters_tupleContext> storeas_parameters_tuple() {
			return getRuleContexts(Storeas_parameters_tupleContext.class);
		}
		public Storeas_parameters_tupleContext storeas_parameters_tuple(int i) {
			return getRuleContext(Storeas_parameters_tupleContext.class,i);
		}
		public TerminalNode RIGHT_PARAN() { return getToken(ConnectorParser.RIGHT_PARAN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(ConnectorParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(ConnectorParser.COMMA, i);
		}
		public Storeas_parametersContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storeas_parameters; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterStoreas_parameters(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitStoreas_parameters(this);
		}
	}

	public final Storeas_parametersContext storeas_parameters() throws RecognitionException {
		Storeas_parametersContext _localctx = new Storeas_parametersContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_storeas_parameters);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(518);
			match(LEFT_PARAN);
			setState(519);
			storeas_parameters_tuple();
			setState(524);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(520);
				match(COMMA);
				setState(521);
				storeas_parameters_tuple();
				}
				}
				setState(526);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(527);
			match(RIGHT_PARAN);
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

	public static class Storeas_parameters_tupleContext extends ParserRuleContext {
		public Storeas_parameterContext storeas_parameter() {
			return getRuleContext(Storeas_parameterContext.class,0);
		}
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public Storeas_valueContext storeas_value() {
			return getRuleContext(Storeas_valueContext.class,0);
		}
		public Storeas_parameters_tupleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storeas_parameters_tuple; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterStoreas_parameters_tuple(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitStoreas_parameters_tuple(this);
		}
	}

	public final Storeas_parameters_tupleContext storeas_parameters_tuple() throws RecognitionException {
		Storeas_parameters_tupleContext _localctx = new Storeas_parameters_tupleContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_storeas_parameters_tuple);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(529);
			storeas_parameter();
			setState(530);
			match(EQUAL);
			setState(531);
			storeas_value();
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

	public static class Storeas_parameterContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public List<TerminalNode> DOT() { return getTokens(ConnectorParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(ConnectorParser.DOT, i);
		}
		public List<TerminalNode> TOPICNAME() { return getTokens(ConnectorParser.TOPICNAME); }
		public TerminalNode TOPICNAME(int i) {
			return getToken(ConnectorParser.TOPICNAME, i);
		}
		public Storeas_parameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storeas_parameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterStoreas_parameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitStoreas_parameter(this);
		}
	}

	public final Storeas_parameterContext storeas_parameter() throws RecognitionException {
		Storeas_parameterContext _localctx = new Storeas_parameterContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_storeas_parameter);
		int _la;
		try {
			setState(539);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(533);
				match(FIELD);
				}
				break;
			case DOT:
			case TOPICNAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(535); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(534);
					_la = _input.LA(1);
					if ( !(_la==DOT || _la==TOPICNAME) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(537); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==TOPICNAME );
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

	public static class Storeas_valueContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public List<TerminalNode> DOT() { return getTokens(ConnectorParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(ConnectorParser.DOT, i);
		}
		public List<TerminalNode> TOPICNAME() { return getTokens(ConnectorParser.TOPICNAME); }
		public TerminalNode TOPICNAME(int i) {
			return getToken(ConnectorParser.TOPICNAME, i);
		}
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public Storeas_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storeas_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterStoreas_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitStoreas_value(this);
		}
	}

	public final Storeas_valueContext storeas_value() throws RecognitionException {
		Storeas_valueContext _localctx = new Storeas_valueContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_storeas_value);
		int _la;
		try {
			setState(548);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(541);
				match(FIELD);
				}
				break;
			case DOT:
			case TOPICNAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(543); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(542);
					_la = _input.LA(1);
					if ( !(_la==DOT || _la==TOPICNAME) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(545); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==TOPICNAME );
				}
				break;
			case INT:
				enterOuterAlt(_localctx, 3);
				{
				setState(547);
				match(INT);
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

	public static class With_tagsContext extends ParserRuleContext {
		public TerminalNode WITHTAG() { return getToken(ConnectorParser.WITHTAG, 0); }
		public TerminalNode LEFT_PARAN() { return getToken(ConnectorParser.LEFT_PARAN, 0); }
		public List<Tag_definitionContext> tag_definition() {
			return getRuleContexts(Tag_definitionContext.class);
		}
		public Tag_definitionContext tag_definition(int i) {
			return getRuleContext(Tag_definitionContext.class,i);
		}
		public TerminalNode RIGHT_PARAN() { return getToken(ConnectorParser.RIGHT_PARAN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(ConnectorParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(ConnectorParser.COMMA, i);
		}
		public With_tagsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_tags; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_tags(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_tags(this);
		}
	}

	public final With_tagsContext with_tags() throws RecognitionException {
		With_tagsContext _localctx = new With_tagsContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_with_tags);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(550);
			match(WITHTAG);
			{
			setState(551);
			match(LEFT_PARAN);
			setState(552);
			tag_definition();
			setState(557);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(553);
				match(COMMA);
				setState(554);
				tag_definition();
				}
				}
				setState(559);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(560);
			match(RIGHT_PARAN);
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

	public static class With_inc_modeContext extends ParserRuleContext {
		public TerminalNode INCREMENTALMODE() { return getToken(ConnectorParser.INCREMENTALMODE, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public Inc_modeContext inc_mode() {
			return getRuleContext(Inc_modeContext.class,0);
		}
		public With_inc_modeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_inc_mode; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_inc_mode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_inc_mode(this);
		}
	}

	public final With_inc_modeContext with_inc_mode() throws RecognitionException {
		With_inc_modeContext _localctx = new With_inc_modeContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_with_inc_mode);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(562);
			match(INCREMENTALMODE);
			setState(563);
			match(EQUAL);
			setState(564);
			inc_mode();
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

	public static class Inc_modeContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public TerminalNode TOPICNAME() { return getToken(ConnectorParser.TOPICNAME, 0); }
		public Inc_modeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inc_mode; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterInc_mode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitInc_mode(this);
		}
	}

	public final Inc_modeContext inc_mode() throws RecognitionException {
		Inc_modeContext _localctx = new Inc_modeContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_inc_mode);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(566);
			_la = _input.LA(1);
			if ( !(_la==FIELD || _la==TOPICNAME) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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

	public static class With_typeContext extends ParserRuleContext {
		public TerminalNode WITHTYPE() { return getToken(ConnectorParser.WITHTYPE, 0); }
		public With_type_valueContext with_type_value() {
			return getRuleContext(With_type_valueContext.class,0);
		}
		public With_typeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_type(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_type(this);
		}
	}

	public final With_typeContext with_type() throws RecognitionException {
		With_typeContext _localctx = new With_typeContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_with_type);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(568);
			match(WITHTYPE);
			setState(569);
			with_type_value();
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

	public static class With_type_valueContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public With_type_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_type_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_type_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_type_value(this);
		}
	}

	public final With_type_valueContext with_type_value() throws RecognitionException {
		With_type_valueContext _localctx = new With_type_valueContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_with_type_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(571);
			match(FIELD);
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

	public static class With_doc_typeContext extends ParserRuleContext {
		public TerminalNode WITHDOCTYPE() { return getToken(ConnectorParser.WITHDOCTYPE, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public Doc_typeContext doc_type() {
			return getRuleContext(Doc_typeContext.class,0);
		}
		public With_doc_typeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_doc_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_doc_type(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_doc_type(this);
		}
	}

	public final With_doc_typeContext with_doc_type() throws RecognitionException {
		With_doc_typeContext _localctx = new With_doc_typeContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_with_doc_type);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(573);
			match(WITHDOCTYPE);
			setState(574);
			match(EQUAL);
			setState(575);
			doc_type();
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

	public static class Doc_typeContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public TerminalNode TOPICNAME() { return getToken(ConnectorParser.TOPICNAME, 0); }
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public Doc_typeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_doc_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterDoc_type(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitDoc_type(this);
		}
	}

	public final Doc_typeContext doc_type() throws RecognitionException {
		Doc_typeContext _localctx = new Doc_typeContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_doc_type);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(577);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << INT) | (1L << FIELD) | (1L << TOPICNAME))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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

	public static class With_index_suffixContext extends ParserRuleContext {
		public TerminalNode WITHINDEXSUFFIX() { return getToken(ConnectorParser.WITHINDEXSUFFIX, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public Index_suffixContext index_suffix() {
			return getRuleContext(Index_suffixContext.class,0);
		}
		public With_index_suffixContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_index_suffix; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_index_suffix(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_index_suffix(this);
		}
	}

	public final With_index_suffixContext with_index_suffix() throws RecognitionException {
		With_index_suffixContext _localctx = new With_index_suffixContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_with_index_suffix);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(579);
			match(WITHINDEXSUFFIX);
			setState(580);
			match(EQUAL);
			setState(581);
			index_suffix();
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

	public static class Index_suffixContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public TerminalNode TOPICNAME() { return getToken(ConnectorParser.TOPICNAME, 0); }
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public Index_suffixContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_suffix; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterIndex_suffix(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitIndex_suffix(this);
		}
	}

	public final Index_suffixContext index_suffix() throws RecognitionException {
		Index_suffixContext _localctx = new Index_suffixContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_index_suffix);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(583);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << INT) | (1L << FIELD) | (1L << TOPICNAME))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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

	public static class With_converterContext extends ParserRuleContext {
		public TerminalNode WITHCONVERTER() { return getToken(ConnectorParser.WITHCONVERTER, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public With_converter_valueContext with_converter_value() {
			return getRuleContext(With_converter_valueContext.class,0);
		}
		public With_converterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_converter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_converter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_converter(this);
		}
	}

	public final With_converterContext with_converter() throws RecognitionException {
		With_converterContext _localctx = new With_converterContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_with_converter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(585);
			match(WITHCONVERTER);
			setState(586);
			match(EQUAL);
			setState(587);
			with_converter_value();
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

	public static class With_converter_valueContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public TerminalNode TOPICNAME() { return getToken(ConnectorParser.TOPICNAME, 0); }
		public With_converter_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_converter_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_converter_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_converter_value(this);
		}
	}

	public final With_converter_valueContext with_converter_value() throws RecognitionException {
		With_converter_valueContext _localctx = new With_converter_valueContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_with_converter_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(589);
			_la = _input.LA(1);
			if ( !(_la==TOPICNAME || _la==ID) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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

	public static class With_jms_selectorContext extends ParserRuleContext {
		public TerminalNode WITHJMSSELECTOR() { return getToken(ConnectorParser.WITHJMSSELECTOR, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public Jms_selector_valueContext jms_selector_value() {
			return getRuleContext(Jms_selector_valueContext.class,0);
		}
		public With_jms_selectorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_jms_selector; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_jms_selector(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_jms_selector(this);
		}
	}

	public final With_jms_selectorContext with_jms_selector() throws RecognitionException {
		With_jms_selectorContext _localctx = new With_jms_selectorContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_with_jms_selector);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(591);
			match(WITHJMSSELECTOR);
			setState(592);
			match(EQUAL);
			setState(593);
			jms_selector_value();
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

	public static class Jms_selector_valueContext extends ParserRuleContext {
		public TerminalNode ID() { return getToken(ConnectorParser.ID, 0); }
		public TerminalNode TOPICNAME() { return getToken(ConnectorParser.TOPICNAME, 0); }
		public Jms_selector_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jms_selector_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterJms_selector_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitJms_selector_value(this);
		}
	}

	public final Jms_selector_valueContext jms_selector_value() throws RecognitionException {
		Jms_selector_valueContext _localctx = new Jms_selector_valueContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_jms_selector_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(595);
			_la = _input.LA(1);
			if ( !(_la==TOPICNAME || _la==ID) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
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

	public static class Tag_definitionContext extends ParserRuleContext {
		public Tag_keyContext tag_key() {
			return getRuleContext(Tag_keyContext.class,0);
		}
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public Tag_valueContext tag_value() {
			return getRuleContext(Tag_valueContext.class,0);
		}
		public Tag_definitionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tag_definition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterTag_definition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitTag_definition(this);
		}
	}

	public final Tag_definitionContext tag_definition() throws RecognitionException {
		Tag_definitionContext _localctx = new Tag_definitionContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_tag_definition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(597);
			tag_key();
			setState(600);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==EQUAL) {
				{
				setState(598);
				match(EQUAL);
				setState(599);
				tag_value();
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

	public static class Tag_keyContext extends ParserRuleContext {
		public List<TerminalNode> FIELD() { return getTokens(ConnectorParser.FIELD); }
		public TerminalNode FIELD(int i) {
			return getToken(ConnectorParser.FIELD, i);
		}
		public List<TerminalNode> DOT() { return getTokens(ConnectorParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(ConnectorParser.DOT, i);
		}
		public List<TerminalNode> TOPICNAME() { return getTokens(ConnectorParser.TOPICNAME); }
		public TerminalNode TOPICNAME(int i) {
			return getToken(ConnectorParser.TOPICNAME, i);
		}
		public Tag_keyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tag_key; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterTag_key(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitTag_key(this);
		}
	}

	public final Tag_keyContext tag_key() throws RecognitionException {
		Tag_keyContext _localctx = new Tag_keyContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_tag_key);
		int _la;
		try {
			setState(612);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,67,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(603); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(602);
					_la = _input.LA(1);
					if ( !(_la==DOT || _la==FIELD) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(605); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==FIELD );
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(608); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(607);
					_la = _input.LA(1);
					if ( !(_la==DOT || _la==TOPICNAME) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(610); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==TOPICNAME );
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

	public static class Tag_valueContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public List<TerminalNode> DOT() { return getTokens(ConnectorParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(ConnectorParser.DOT, i);
		}
		public List<TerminalNode> TOPICNAME() { return getTokens(ConnectorParser.TOPICNAME); }
		public TerminalNode TOPICNAME(int i) {
			return getToken(ConnectorParser.TOPICNAME, i);
		}
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public Tag_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tag_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterTag_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitTag_value(this);
		}
	}

	public final Tag_valueContext tag_value() throws RecognitionException {
		Tag_valueContext _localctx = new Tag_valueContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_tag_value);
		int _la;
		try {
			setState(621);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(614);
				match(FIELD);
				}
				break;
			case DOT:
			case TOPICNAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(616); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(615);
					_la = _input.LA(1);
					if ( !(_la==DOT || _la==TOPICNAME) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					}
					}
					setState(618); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==TOPICNAME );
				}
				break;
			case INT:
				enterOuterAlt(_localctx, 3);
				{
				setState(620);
				match(INT);
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

	public static class Ttl_clauseContext extends ParserRuleContext {
		public TerminalNode TTL() { return getToken(ConnectorParser.TTL, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public Ttl_typeContext ttl_type() {
			return getRuleContext(Ttl_typeContext.class,0);
		}
		public Ttl_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ttl_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterTtl_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitTtl_clause(this);
		}
	}

	public final Ttl_clauseContext ttl_clause() throws RecognitionException {
		Ttl_clauseContext _localctx = new Ttl_clauseContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_ttl_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(623);
			match(TTL);
			setState(624);
			match(EQUAL);
			setState(625);
			ttl_type();
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

	public static class Ttl_typeContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public Ttl_typeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ttl_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterTtl_type(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitTtl_type(this);
		}
	}

	public final Ttl_typeContext ttl_type() throws RecognitionException {
		Ttl_typeContext _localctx = new Ttl_typeContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_ttl_type);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(627);
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

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\64\u0278\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\3\2\3"+
		"\2\5\2\u00a9\n\2\3\3\3\3\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3"+
		"\7\3\7\3\b\3\b\3\b\5\b\u00bd\n\b\3\t\3\t\3\n\3\n\3\n\3\n\5\n\u00c5\n\n"+
		"\3\n\5\n\u00c8\n\n\3\n\3\n\5\n\u00cc\n\n\3\n\5\n\u00cf\n\n\3\n\5\n\u00d2"+
		"\n\n\3\n\5\n\u00d5\n\n\3\n\5\n\u00d8\n\n\3\n\5\n\u00db\n\n\3\n\5\n\u00de"+
		"\n\n\3\n\5\n\u00e1\n\n\3\n\5\n\u00e4\n\n\3\n\5\n\u00e7\n\n\3\n\5\n\u00ea"+
		"\n\n\3\n\5\n\u00ed\n\n\3\n\5\n\u00f0\n\n\3\n\5\n\u00f3\n\n\3\n\5\n\u00f6"+
		"\n\n\3\n\5\n\u00f9\n\n\3\n\5\n\u00fc\n\n\3\n\5\n\u00ff\n\n\3\n\5\n\u0102"+
		"\n\n\3\n\5\n\u0105\n\n\3\n\5\n\u0108\n\n\3\13\3\13\3\13\5\13\u010d\n\13"+
		"\3\13\5\13\u0110\n\13\3\13\5\13\u0113\n\13\3\13\5\13\u0116\n\13\3\13\5"+
		"\13\u0119\n\13\3\13\5\13\u011c\n\13\3\13\5\13\u011f\n\13\3\13\5\13\u0122"+
		"\n\13\3\13\5\13\u0125\n\13\3\13\5\13\u0128\n\13\3\13\5\13\u012b\n\13\3"+
		"\13\5\13\u012e\n\13\3\13\5\13\u0131\n\13\3\13\5\13\u0134\n\13\3\13\5\13"+
		"\u0137\n\13\3\f\3\f\3\f\3\f\3\f\5\f\u013e\n\f\3\r\6\r\u0141\n\r\r\r\16"+
		"\r\u0142\3\16\6\16\u0146\n\16\r\16\16\16\u0147\3\17\3\17\3\17\5\17\u014d"+
		"\n\17\3\17\5\17\u0150\n\17\3\20\3\20\3\20\7\20\u0155\n\20\f\20\16\20\u0158"+
		"\13\20\3\20\3\20\5\20\u015c\n\20\3\21\3\21\3\22\3\22\3\22\7\22\u0163\n"+
		"\22\f\22\16\22\u0166\13\22\3\23\3\23\3\23\3\24\3\24\3\25\3\25\3\25\3\26"+
		"\3\26\3\26\7\26\u0173\n\26\f\26\16\26\u0176\13\26\3\27\3\27\3\30\3\30"+
		"\3\30\7\30\u017d\n\30\f\30\16\30\u0180\13\30\3\31\3\31\3\32\3\32\3\33"+
		"\3\33\3\34\3\34\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3 \7 \u0195"+
		"\n \f \16 \u0198\13 \3!\3!\3!\3\"\3\"\3#\3#\3#\7#\u01a2\n#\f#\16#\u01a5"+
		"\13#\3$\3$\3$\3$\3$\3$\3%\3%\3%\3&\3&\3\'\3\'\3(\3(\3)\3)\3)\7)\u01b9"+
		"\n)\f)\16)\u01bc\13)\3*\3*\3*\3*\3*\3*\3+\3+\3+\3,\3,\3-\3-\3-\5-\u01cc"+
		"\n-\3.\3.\3.\3.\3/\3/\3/\7/\u01d5\n/\f/\16/\u01d8\13/\3\60\3\60\3\60\3"+
		"\61\3\61\3\61\3\62\3\62\3\63\3\63\3\63\3\63\3\63\3\64\3\64\3\65\3\65\3"+
		"\66\3\66\3\67\3\67\3\67\38\38\39\39\3:\3:\3:\3;\3;\3<\3<\3<\7<\u01fc\n"+
		"<\f<\16<\u01ff\13<\3=\3=\6=\u0203\n=\r=\16=\u0204\5=\u0207\n=\3>\3>\3"+
		">\3>\7>\u020d\n>\f>\16>\u0210\13>\3>\3>\3?\3?\3?\3?\3@\3@\6@\u021a\n@"+
		"\r@\16@\u021b\5@\u021e\n@\3A\3A\6A\u0222\nA\rA\16A\u0223\3A\5A\u0227\n"+
		"A\3B\3B\3B\3B\3B\7B\u022e\nB\fB\16B\u0231\13B\3B\3B\3C\3C\3C\3C\3D\3D"+
		"\3E\3E\3E\3F\3F\3G\3G\3G\3G\3H\3H\3I\3I\3I\3I\3J\3J\3K\3K\3K\3K\3L\3L"+
		"\3M\3M\3M\3M\3N\3N\3O\3O\3O\5O\u025b\nO\3P\6P\u025e\nP\rP\16P\u025f\3"+
		"P\6P\u0263\nP\rP\16P\u0264\5P\u0267\nP\3Q\3Q\6Q\u026b\nQ\rQ\16Q\u026c"+
		"\3Q\5Q\u0270\nQ\3R\3R\3R\3R\3S\3S\3S\2\2T\2\4\6\b\n\f\16\20\22\24\26\30"+
		"\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080"+
		"\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098"+
		"\u009a\u009c\u009e\u00a0\u00a2\u00a4\2\t\4\2--\60\61\3\2\60\61\4\2\24"+
		"\24\60\60\4\2**\60\61\4\2--\61\61\4\2\61\61\64\64\4\2--\60\60\2\u026e"+
		"\2\u00a8\3\2\2\2\4\u00aa\3\2\2\2\6\u00ac\3\2\2\2\b\u00ae\3\2\2\2\n\u00b1"+
		"\3\2\2\2\f\u00b4\3\2\2\2\16\u00bc\3\2\2\2\20\u00be\3\2\2\2\22\u00c0\3"+
		"\2\2\2\24\u0109\3\2\2\2\26\u0138\3\2\2\2\30\u0140\3\2\2\2\32\u0145\3\2"+
		"\2\2\34\u014f\3\2\2\2\36\u0151\3\2\2\2 \u015d\3\2\2\2\"\u015f\3\2\2\2"+
		"$\u0167\3\2\2\2&\u016a\3\2\2\2(\u016c\3\2\2\2*\u016f\3\2\2\2,\u0177\3"+
		"\2\2\2.\u0179\3\2\2\2\60\u0181\3\2\2\2\62\u0183\3\2\2\2\64\u0185\3\2\2"+
		"\2\66\u0187\3\2\2\28\u018b\3\2\2\2:\u018d\3\2\2\2<\u018f\3\2\2\2>\u0191"+
		"\3\2\2\2@\u0199\3\2\2\2B\u019c\3\2\2\2D\u019e\3\2\2\2F\u01a6\3\2\2\2H"+
		"\u01ac\3\2\2\2J\u01af\3\2\2\2L\u01b1\3\2\2\2N\u01b3\3\2\2\2P\u01b5\3\2"+
		"\2\2R\u01bd\3\2\2\2T\u01c3\3\2\2\2V\u01c6\3\2\2\2X\u01c8\3\2\2\2Z\u01cd"+
		"\3\2\2\2\\\u01d1\3\2\2\2^\u01d9\3\2\2\2`\u01dc\3\2\2\2b\u01df\3\2\2\2"+
		"d\u01e1\3\2\2\2f\u01e6\3\2\2\2h\u01e8\3\2\2\2j\u01ea\3\2\2\2l\u01ec\3"+
		"\2\2\2n\u01ef\3\2\2\2p\u01f1\3\2\2\2r\u01f3\3\2\2\2t\u01f6\3\2\2\2v\u01f8"+
		"\3\2\2\2x\u0206\3\2\2\2z\u0208\3\2\2\2|\u0213\3\2\2\2~\u021d\3\2\2\2\u0080"+
		"\u0226\3\2\2\2\u0082\u0228\3\2\2\2\u0084\u0234\3\2\2\2\u0086\u0238\3\2"+
		"\2\2\u0088\u023a\3\2\2\2\u008a\u023d\3\2\2\2\u008c\u023f\3\2\2\2\u008e"+
		"\u0243\3\2\2\2\u0090\u0245\3\2\2\2\u0092\u0249\3\2\2\2\u0094\u024b\3\2"+
		"\2\2\u0096\u024f\3\2\2\2\u0098\u0251\3\2\2\2\u009a\u0255\3\2\2\2\u009c"+
		"\u0257\3\2\2\2\u009e\u0266\3\2\2\2\u00a0\u026f\3\2\2\2\u00a2\u0271\3\2"+
		"\2\2\u00a4\u0275\3\2\2\2\u00a6\u00a9\5\22\n\2\u00a7\u00a9\5\24\13\2\u00a8"+
		"\u00a6\3\2\2\2\u00a8\u00a7\3\2\2\2\u00a9\3\3\2\2\2\u00aa\u00ab\7\5\2\2"+
		"\u00ab\5\3\2\2\2\u00ac\u00ad\7\32\2\2\u00ad\7\3\2\2\2\u00ae\u00af\7\3"+
		"\2\2\u00af\u00b0\5\4\3\2\u00b0\t\3\2\2\2\u00b1\u00b2\7\4\2\2\u00b2\u00b3"+
		"\5\4\3\2\u00b3\13\3\2\2\2\u00b4\u00b5\7\4\2\2\u00b5\u00b6\5\6\4\2\u00b6"+
		"\u00b7\7\60\2\2\u00b7\u00b8\5\4\3\2\u00b8\r\3\2\2\2\u00b9\u00bd\5\b\5"+
		"\2\u00ba\u00bd\5\n\6\2\u00bb\u00bd\5\f\7\2\u00bc\u00b9\3\2\2\2\u00bc\u00ba"+
		"\3\2\2\2\u00bc\u00bb\3\2\2\2\u00bd\17\3\2\2\2\u00be\u00bf\7\60\2\2\u00bf"+
		"\21\3\2\2\2\u00c0\u00c1\5\16\b\2\u00c1\u00c2\5\32\16\2\u00c2\u00c4\5\26"+
		"\f\2\u00c3\u00c5\5\60\31\2\u00c4\u00c3\3\2\2\2\u00c4\u00c5\3\2\2\2\u00c5"+
		"\u00c7\3\2\2\2\u00c6\u00c8\5n8\2\u00c7\u00c6\3\2\2\2\u00c7\u00c8\3\2\2"+
		"\2\u00c8\u00cb\3\2\2\2\u00c9\u00ca\7\32\2\2\u00ca\u00cc\5.\30\2\u00cb"+
		"\u00c9\3\2\2\2\u00cb\u00cc\3\2\2\2\u00cc\u00ce\3\2\2\2\u00cd\u00cf\5\62"+
		"\32\2\u00ce\u00cd\3\2\2\2\u00ce\u00cf\3\2\2\2\u00cf\u00d1\3\2\2\2\u00d0"+
		"\u00d2\5\66\34\2\u00d1\u00d0\3\2\2\2\u00d1\u00d2\3\2\2\2\u00d2\u00d4\3"+
		"\2\2\2\u00d3\u00d5\58\35\2\u00d4\u00d3\3\2\2\2\u00d4\u00d5\3\2\2\2\u00d5"+
		"\u00d7\3\2\2\2\u00d6\u00d8\5:\36\2\u00d7\u00d6\3\2\2\2\u00d7\u00d8\3\2"+
		"\2\2\u00d8\u00da\3\2\2\2\u00d9\u00db\5r:\2\u00da\u00d9\3\2\2\2\u00da\u00db"+
		"\3\2\2\2\u00db\u00dd\3\2\2\2\u00dc\u00de\5@!\2\u00dd\u00dc\3\2\2\2\u00dd"+
		"\u00de\3\2\2\2\u00de\u00e0\3\2\2\2\u00df\u00e1\5F$\2\u00e0\u00df\3\2\2"+
		"\2\u00e0\u00e1\3\2\2\2\u00e1\u00e3\3\2\2\2\u00e2\u00e4\5R*\2\u00e3\u00e2"+
		"\3\2\2\2\u00e3\u00e4\3\2\2\2\u00e4\u00e6\3\2\2\2\u00e5\u00e7\5H%\2\u00e6"+
		"\u00e5\3\2\2\2\u00e6\u00e7\3\2\2\2\u00e7\u00e9\3\2\2\2\u00e8\u00ea\5l"+
		"\67\2\u00e9\u00e8\3\2\2\2\u00e9\u00ea\3\2\2\2\u00ea\u00ec\3\2\2\2\u00eb"+
		"\u00ed\5j\66\2\u00ec\u00eb\3\2\2\2\u00ec\u00ed\3\2\2\2\u00ed\u00ef\3\2"+
		"\2\2\u00ee\u00f0\5v<\2\u00ef\u00ee\3\2\2\2\u00ef\u00f0\3\2\2\2\u00f0\u00f2"+
		"\3\2\2\2\u00f1\u00f3\5\u0082B\2\u00f2\u00f1\3\2\2\2\u00f2\u00f3\3\2\2"+
		"\2\u00f3\u00f5\3\2\2\2\u00f4\u00f6\5\u0084C\2\u00f5\u00f4\3\2\2\2\u00f5"+
		"\u00f6\3\2\2\2\u00f6\u00f8\3\2\2\2\u00f7\u00f9\5\u0088E\2\u00f8\u00f7"+
		"\3\2\2\2\u00f8\u00f9\3\2\2\2\u00f9\u00fb\3\2\2\2\u00fa\u00fc\5\u008cG"+
		"\2\u00fb\u00fa\3\2\2\2\u00fb\u00fc\3\2\2\2\u00fc\u00fe\3\2\2\2\u00fd\u00ff"+
		"\5\u0090I\2\u00fe\u00fd\3\2\2\2\u00fe\u00ff\3\2\2\2\u00ff\u0101\3\2\2"+
		"\2\u0100\u0102\5\u00a2R\2\u0101\u0100\3\2\2\2\u0101\u0102\3\2\2\2\u0102"+
		"\u0104\3\2\2\2\u0103\u0105\5\u0094K\2\u0104\u0103\3\2\2\2\u0104\u0105"+
		"\3\2\2\2\u0105\u0107\3\2\2\2\u0106\u0108\5\u0098M\2\u0107\u0106\3\2\2"+
		"\2\u0107\u0108\3\2\2\2\u0108\23\3\2\2\2\u0109\u010c\5\26\f\2\u010a\u010b"+
		"\7\32\2\2\u010b\u010d\5.\30\2\u010c\u010a\3\2\2\2\u010c\u010d\3\2\2\2"+
		"\u010d\u010f\3\2\2\2\u010e\u0110\5n8\2\u010f\u010e\3\2\2\2\u010f\u0110"+
		"\3\2\2\2\u0110\u0112\3\2\2\2\u0111\u0113\5l\67\2\u0112\u0111\3\2\2\2\u0112"+
		"\u0113\3\2\2\2\u0113\u0115\3\2\2\2\u0114\u0116\5j\66\2\u0115\u0114\3\2"+
		"\2\2\u0115\u0116\3\2\2\2\u0116\u0118\3\2\2\2\u0117\u0119\5T+\2\u0118\u0117"+
		"\3\2\2\2\u0118\u0119\3\2\2\2\u0119\u011b\3\2\2\2\u011a\u011c\5^\60\2\u011b"+
		"\u011a\3\2\2\2\u011b\u011c\3\2\2\2\u011c\u011e\3\2\2\2\u011d\u011f\5d"+
		"\63\2\u011e\u011d\3\2\2\2\u011e\u011f\3\2\2\2\u011f\u0121\3\2\2\2\u0120"+
		"\u0122\5`\61\2\u0121\u0120\3\2\2\2\u0121\u0122\3\2\2\2\u0122\u0124\3\2"+
		"\2\2\u0123\u0125\5v<\2\u0124\u0123\3\2\2\2\u0124\u0125\3\2\2\2\u0125\u0127"+
		"\3\2\2\2\u0126\u0128\5\u0082B\2\u0127\u0126\3\2\2\2\u0127\u0128\3\2\2"+
		"\2\u0128\u012a\3\2\2\2\u0129\u012b\5\u0084C\2\u012a\u0129\3\2\2\2\u012a"+
		"\u012b\3\2\2\2\u012b\u012d\3\2\2\2\u012c\u012e\5\u008cG\2\u012d\u012c"+
		"\3\2\2\2\u012d\u012e\3\2\2\2\u012e\u0130\3\2\2\2\u012f\u0131\5\u0090I"+
		"\2\u0130\u012f\3\2\2\2\u0130\u0131\3\2\2\2\u0131\u0133\3\2\2\2\u0132\u0134"+
		"\5\u0094K\2\u0133\u0132\3\2\2\2\u0133\u0134\3\2\2\2\u0134\u0136\3\2\2"+
		"\2\u0135\u0137\5\u0098M\2\u0136\u0135\3\2\2\2\u0136\u0137\3\2\2\2\u0137"+
		"\25\3\2\2\2\u0138\u0139\7\6\2\2\u0139\u013a\5\"\22\2\u013a\u013b\7\7\2"+
		"\2\u013b\u013d\5\30\r\2\u013c\u013e\5(\25\2\u013d\u013c\3\2\2\2\u013d"+
		"\u013e\3\2\2\2\u013e\27\3\2\2\2\u013f\u0141\t\2\2\2\u0140\u013f\3\2\2"+
		"\2\u0141\u0142\3\2\2\2\u0142\u0140\3\2\2\2\u0142\u0143\3\2\2\2\u0143\31"+
		"\3\2\2\2\u0144\u0146\t\2\2\2\u0145\u0144\3\2\2\2\u0146\u0147\3\2\2\2\u0147"+
		"\u0145\3\2\2\2\u0147\u0148\3\2\2\2\u0148\33\3\2\2\2\u0149\u014c\5\36\20"+
		"\2\u014a\u014b\7\t\2\2\u014b\u014d\5 \21\2\u014c\u014a\3\2\2\2\u014c\u014d"+
		"\3\2\2\2\u014d\u0150\3\2\2\2\u014e\u0150\7+\2\2\u014f\u0149\3\2\2\2\u014f"+
		"\u014e\3\2\2\2\u0150\35\3\2\2\2\u0151\u0156\7\60\2\2\u0152\u0153\7-\2"+
		"\2\u0153\u0155\7\60\2\2\u0154\u0152\3\2\2\2\u0155\u0158\3\2\2\2\u0156"+
		"\u0154\3\2\2\2\u0156\u0157\3\2\2\2\u0157\u015b\3\2\2\2\u0158\u0156\3\2"+
		"\2\2\u0159\u015a\7-\2\2\u015a\u015c\7+\2\2\u015b\u0159\3\2\2\2\u015b\u015c"+
		"\3\2\2\2\u015c\37\3\2\2\2\u015d\u015e\7\60\2\2\u015e!\3\2\2\2\u015f\u0164"+
		"\5\34\17\2\u0160\u0161\7,\2\2\u0161\u0163\5\34\17\2\u0162\u0160\3\2\2"+
		"\2\u0163\u0166\3\2\2\2\u0164\u0162\3\2\2\2\u0164\u0165\3\2\2\2\u0165#"+
		"\3\2\2\2\u0166\u0164\3\2\2\2\u0167\u0168\7\7\2\2\u0168\u0169\5\32\16\2"+
		"\u0169%\3\2\2\2\u016a\u016b\t\3\2\2\u016b\'\3\2\2\2\u016c\u016d\7\b\2"+
		"\2\u016d\u016e\5*\26\2\u016e)\3\2\2\2\u016f\u0174\5\34\17\2\u0170\u0171"+
		"\7,\2\2\u0171\u0173\5\34\17\2\u0172\u0170\3\2\2\2\u0173\u0176\3\2\2\2"+
		"\u0174\u0172\3\2\2\2\u0174\u0175\3\2\2\2\u0175+\3\2\2\2\u0176\u0174\3"+
		"\2\2\2\u0177\u0178\5\36\20\2\u0178-\3\2\2\2\u0179\u017e\5,\27\2\u017a"+
		"\u017b\7,\2\2\u017b\u017d\5,\27\2\u017c\u017a\3\2\2\2\u017d\u0180\3\2"+
		"\2\2\u017e\u017c\3\2\2\2\u017e\u017f\3\2\2\2\u017f/\3\2\2\2\u0180\u017e"+
		"\3\2\2\2\u0181\u0182\7\n\2\2\u0182\61\3\2\2\2\u0183\u0184\7\13\2\2\u0184"+
		"\63\3\2\2\2\u0185\u0186\7*\2\2\u0186\65\3\2\2\2\u0187\u0188\7\16\2\2\u0188"+
		"\u0189\7)\2\2\u0189\u018a\5\64\33\2\u018a\67\3\2\2\2\u018b\u018c\7\17"+
		"\2\2\u018c9\3\2\2\2\u018d\u018e\7\20\2\2\u018e;\3\2\2\2\u018f\u0190\7"+
		"\60\2\2\u0190=\3\2\2\2\u0191\u0196\5<\37\2\u0192\u0193\7,\2\2\u0193\u0195"+
		"\5<\37\2\u0194\u0192\3\2\2\2\u0195\u0198\3\2\2\2\u0196\u0194\3\2\2\2\u0196"+
		"\u0197\3\2\2\2\u0197?\3\2\2\2\u0198\u0196\3\2\2\2\u0199\u019a\7\21\2\2"+
		"\u019a\u019b\5> \2\u019bA\3\2\2\2\u019c\u019d\7\60\2\2\u019dC\3\2\2\2"+
		"\u019e\u01a3\5B\"\2\u019f\u01a0\7,\2\2\u01a0\u01a2\5B\"\2\u01a1\u019f"+
		"\3\2\2\2\u01a2\u01a5\3\2\2\2\u01a3\u01a1\3\2\2\2\u01a3\u01a4\3\2\2\2\u01a4"+
		"E\3\2\2\2\u01a5\u01a3\3\2\2\2\u01a6\u01a7\7\22\2\2\u01a7\u01a8\5D#\2\u01a8"+
		"\u01a9\7\5\2\2\u01a9\u01aa\5L\'\2\u01aa\u01ab\7\r\2\2\u01abG\3\2\2\2\u01ac"+
		"\u01ad\7\23\2\2\u01ad\u01ae\5J&\2\u01aeI\3\2\2\2\u01af\u01b0\t\4\2\2\u01b0"+
		"K\3\2\2\2\u01b1\u01b2\7*\2\2\u01b2M\3\2\2\2\u01b3\u01b4\7\60\2\2\u01b4"+
		"O\3\2\2\2\u01b5\u01ba\5N(\2\u01b6\u01b7\7,\2\2\u01b7\u01b9\5N(\2\u01b8"+
		"\u01b6\3\2\2\2\u01b9\u01bc\3\2\2\2\u01ba\u01b8\3\2\2\2\u01ba\u01bb\3\2"+
		"\2\2\u01bbQ\3\2\2\2\u01bc\u01ba\3\2\2\2\u01bd\u01be\7\f\2\2\u01be\u01bf"+
		"\5P)\2\u01bf\u01c0\7\5\2\2\u01c0\u01c1\5L\'\2\u01c1\u01c2\7\r\2\2\u01c2"+
		"S\3\2\2\2\u01c3\u01c4\7\25\2\2\u01c4\u01c5\5V,\2\u01c5U\3\2\2\2\u01c6"+
		"\u01c7\t\5\2\2\u01c7W\3\2\2\2\u01c8\u01cb\7*\2\2\u01c9\u01ca\7,\2\2\u01ca"+
		"\u01cc\7*\2\2\u01cb\u01c9\3\2\2\2\u01cb\u01cc\3\2\2\2\u01ccY\3\2\2\2\u01cd"+
		"\u01ce\7.\2\2\u01ce\u01cf\5X-\2\u01cf\u01d0\7/\2\2\u01d0[\3\2\2\2\u01d1"+
		"\u01d6\5Z.\2\u01d2\u01d3\7,\2\2\u01d3\u01d5\5Z.\2\u01d4\u01d2\3\2\2\2"+
		"\u01d5\u01d8\3\2\2\2\u01d6\u01d4\3\2\2\2\u01d6\u01d7\3\2\2\2\u01d7]\3"+
		"\2\2\2\u01d8\u01d6\3\2\2\2\u01d9\u01da\7\26\2\2\u01da\u01db\5\\/\2\u01db"+
		"_\3\2\2\2\u01dc\u01dd\7\"\2\2\u01dd\u01de\5b\62\2\u01dea\3\2\2\2\u01df"+
		"\u01e0\7*\2\2\u01e0c\3\2\2\2\u01e1\u01e2\7\33\2\2\u01e2\u01e3\5f\64\2"+
		"\u01e3\u01e4\7\34\2\2\u01e4\u01e5\5h\65\2\u01e5e\3\2\2\2\u01e6\u01e7\7"+
		"*\2\2\u01e7g\3\2\2\2\u01e8\u01e9\7*\2\2\u01e9i\3\2\2\2\u01ea\u01eb\7\36"+
		"\2\2\u01ebk\3\2\2\2\u01ec\u01ed\7\35\2\2\u01ed\u01ee\5p9\2\u01eem\3\2"+
		"\2\2\u01ef\u01f0\7\30\2\2\u01f0o\3\2\2\2\u01f1\u01f2\7\37\2\2\u01f2q\3"+
		"\2\2\2\u01f3\u01f4\7 \2\2\u01f4\u01f5\5t;\2\u01f5s\3\2\2\2\u01f6\u01f7"+
		"\7*\2\2\u01f7u\3\2\2\2\u01f8\u01f9\7!\2\2\u01f9\u01fd\5x=\2\u01fa\u01fc"+
		"\5z>\2\u01fb\u01fa\3\2\2\2\u01fc\u01ff\3\2\2\2\u01fd\u01fb\3\2\2\2\u01fd"+
		"\u01fe\3\2\2\2\u01few\3\2\2\2\u01ff\u01fd\3\2\2\2\u0200\u0207\7\60\2\2"+
		"\u0201\u0203\t\6\2\2\u0202\u0201\3\2\2\2\u0203\u0204\3\2\2\2\u0204\u0202"+
		"\3\2\2\2\u0204\u0205\3\2\2\2\u0205\u0207\3\2\2\2\u0206\u0200\3\2\2\2\u0206"+
		"\u0202\3\2\2\2\u0207y\3\2\2\2\u0208\u0209\7.\2\2\u0209\u020e\5|?\2\u020a"+
		"\u020b\7,\2\2\u020b\u020d\5|?\2\u020c\u020a\3\2\2\2\u020d\u0210\3\2\2"+
		"\2\u020e\u020c\3\2\2\2\u020e\u020f\3\2\2\2\u020f\u0211\3\2\2\2\u0210\u020e"+
		"\3\2\2\2\u0211\u0212\7/\2\2\u0212{\3\2\2\2\u0213\u0214\5~@\2\u0214\u0215"+
		"\7)\2\2\u0215\u0216\5\u0080A\2\u0216}\3\2\2\2\u0217\u021e\7\60\2\2\u0218"+
		"\u021a\t\6\2\2\u0219\u0218\3\2\2\2\u021a\u021b\3\2\2\2\u021b\u0219\3\2"+
		"\2\2\u021b\u021c\3\2\2\2\u021c\u021e\3\2\2\2\u021d\u0217\3\2\2\2\u021d"+
		"\u0219\3\2\2\2\u021e\177\3\2\2\2\u021f\u0227\7\60\2\2\u0220\u0222\t\6"+
		"\2\2\u0221\u0220\3\2\2\2\u0222\u0223\3\2\2\2\u0223\u0221\3\2\2\2\u0223"+
		"\u0224\3\2\2\2\u0224\u0227\3\2\2\2\u0225\u0227\7*\2\2\u0226\u021f\3\2"+
		"\2\2\u0226\u0221\3\2\2\2\u0226\u0225\3\2\2\2\u0227\u0081\3\2\2\2\u0228"+
		"\u0229\7\27\2\2\u0229\u022a\7.\2\2\u022a\u022f\5\u009cO\2\u022b\u022c"+
		"\7,\2\2\u022c\u022e\5\u009cO\2\u022d\u022b\3\2\2\2\u022e\u0231\3\2\2\2"+
		"\u022f\u022d\3\2\2\2\u022f\u0230\3\2\2\2\u0230\u0232\3\2\2\2\u0231\u022f"+
		"\3\2\2\2\u0232\u0233\7/\2\2\u0233\u0083\3\2\2\2\u0234\u0235\7#\2\2\u0235"+
		"\u0236\7)\2\2\u0236\u0237\5\u0086D\2\u0237\u0085\3\2\2\2\u0238\u0239\t"+
		"\3\2\2\u0239\u0087\3\2\2\2\u023a\u023b\7\31\2\2\u023b\u023c\5\u008aF\2"+
		"\u023c\u0089\3\2\2\2\u023d\u023e\7\60\2\2\u023e\u008b\3\2\2\2\u023f\u0240"+
		"\7$\2\2\u0240\u0241\7)\2\2\u0241\u0242\5\u008eH\2\u0242\u008d\3\2\2\2"+
		"\u0243\u0244\t\5\2\2\u0244\u008f\3\2\2\2\u0245\u0246\7%\2\2\u0246\u0247"+
		"\7)\2\2\u0247\u0248\5\u0092J\2\u0248\u0091\3\2\2\2\u0249\u024a\t\5\2\2"+
		"\u024a\u0093\3\2\2\2\u024b\u024c\7&\2\2\u024c\u024d\7)\2\2\u024d\u024e"+
		"\5\u0096L\2\u024e\u0095\3\2\2\2\u024f\u0250\t\7\2\2\u0250\u0097\3\2\2"+
		"\2\u0251\u0252\7\'\2\2\u0252\u0253\7)\2\2\u0253\u0254\5\u009aN\2\u0254"+
		"\u0099\3\2\2\2\u0255\u0256\t\7\2\2\u0256\u009b\3\2\2\2\u0257\u025a\5\u009e"+
		"P\2\u0258\u0259\7)\2\2\u0259\u025b\5\u00a0Q\2\u025a\u0258\3\2\2\2\u025a"+
		"\u025b\3\2\2\2\u025b\u009d\3\2\2\2\u025c\u025e\t\b\2\2\u025d\u025c\3\2"+
		"\2\2\u025e\u025f\3\2\2\2\u025f\u025d\3\2\2\2\u025f\u0260\3\2\2\2\u0260"+
		"\u0267\3\2\2\2\u0261\u0263\t\6\2\2\u0262\u0261\3\2\2\2\u0263\u0264\3\2"+
		"\2\2\u0264\u0262\3\2\2\2\u0264\u0265\3\2\2\2\u0265\u0267\3\2\2\2\u0266"+
		"\u025d\3\2\2\2\u0266\u0262\3\2\2\2\u0267\u009f\3\2\2\2\u0268\u0270\7\60"+
		"\2\2\u0269\u026b\t\6\2\2\u026a\u0269\3\2\2\2\u026b\u026c\3\2\2\2\u026c"+
		"\u026a\3\2\2\2\u026c\u026d\3\2\2\2\u026d\u0270\3\2\2\2\u026e\u0270\7*"+
		"\2\2\u026f\u0268\3\2\2\2\u026f\u026a\3\2\2\2\u026f\u026e\3\2\2\2\u0270"+
		"\u00a1\3\2\2\2\u0271\u0272\7(\2\2\u0272\u0273\7)\2\2\u0273\u0274\5\u00a4"+
		"S\2\u0274\u00a3\3\2\2\2\u0275\u0276\7*\2\2\u0276\u00a5\3\2\2\2H\u00a8"+
		"\u00bc\u00c4\u00c7\u00cb\u00ce\u00d1\u00d4\u00d7\u00da\u00dd\u00e0\u00e3"+
		"\u00e6\u00e9\u00ec\u00ef\u00f2\u00f5\u00f8\u00fb\u00fe\u0101\u0104\u0107"+
		"\u010c\u010f\u0112\u0115\u0118\u011b\u011e\u0121\u0124\u0127\u012a\u012d"+
		"\u0130\u0133\u0136\u013d\u0142\u0147\u014c\u014f\u0156\u015b\u0164\u0174"+
		"\u017e\u0196\u01a3\u01ba\u01cb\u01d6\u01fd\u0204\u0206\u020e\u021b\u021d"+
		"\u0223\u0226\u022f\u025a\u025f\u0264\u0266\u026c\u026f";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}