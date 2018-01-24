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
		WITHOFFSET=20, WITHTAG=21, WITHKEY=22, KEYDELIM=23, WITHSTRUCTURE=24, 
		WITHTYPE=25, PK=26, SAMPLE=27, EVERY=28, WITHFORMAT=29, WITHUNWRAP=30, 
		FORMAT=31, PROJECTTO=32, STOREAS=33, LIMIT=34, INCREMENTALMODE=35, WITHDOCTYPE=36, 
		WITHINDEXSUFFIX=37, WITHCONVERTER=38, WITHJMSSELECTOR=39, WITHTARGET=40, 
		WITHCOMPRESSION=41, WITHDELAY=42, TIMESTAMPUNIT=43, WITHPIPELINE=44, TTL=45, 
		EQUAL=46, INT=47, ASTERISK=48, COMMA=49, DOT=50, LEFT_PARAN=51, RIGHT_PARAN=52, 
		FIELD=53, TOPICNAME=54, KEYDELIMVALUE=55, NEWLINE=56, WS=57, ID=58;
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
		RULE_timestamp_value = 36, RULE_timestamp_unit_clause = 37, RULE_timestamp_unit_value = 38, 
		RULE_buckets_number = 39, RULE_clusterby_name = 40, RULE_clusterby_list = 41, 
		RULE_clusterby = 42, RULE_with_consumer_group = 43, RULE_with_consumer_group_value = 44, 
		RULE_offset_partition_inner = 45, RULE_offset_partition = 46, RULE_partition_offset_list = 47, 
		RULE_with_offset_list = 48, RULE_limit_clause = 49, RULE_limit_value = 50, 
		RULE_sample_clause = 51, RULE_sample_value = 52, RULE_sample_period = 53, 
		RULE_with_unwrap_clause = 54, RULE_with_format_clause = 55, RULE_with_structure = 56, 
		RULE_with_format = 57, RULE_project_to = 58, RULE_version_number = 59, 
		RULE_storeas_clause = 60, RULE_storeas_type = 61, RULE_storeas_parameters = 62, 
		RULE_storeas_parameters_tuple = 63, RULE_storeas_parameter = 64, RULE_storeas_value = 65, 
		RULE_with_tags = 66, RULE_with_key = 67, RULE_with_key_value = 68, RULE_key_delimiter = 69, 
		RULE_key_delimiter_value = 70, RULE_with_inc_mode = 71, RULE_inc_mode = 72, 
		RULE_with_type = 73, RULE_with_type_value = 74, RULE_with_doc_type = 75, 
		RULE_doc_type = 76, RULE_with_index_suffix = 77, RULE_index_suffix = 78, 
		RULE_with_converter = 79, RULE_with_converter_value = 80, RULE_with_target = 81, 
		RULE_with_target_value = 82, RULE_with_jms_selector = 83, RULE_jms_selector_value = 84, 
		RULE_tag_definition = 85, RULE_tag_key = 86, RULE_tag_value = 87, RULE_ttl_clause = 88, 
		RULE_ttl_type = 89, RULE_with_pipeline_clause = 90, RULE_pipeline_value = 91, 
		RULE_with_compression_clause = 92, RULE_with_compression_type = 93, RULE_with_delay_clause = 94, 
		RULE_with_delay_value = 95;
	public static final String[] ruleNames = {
		"stat", "into", "pk", "insert_into", "upsert_into", "upsert_pk_into", 
		"write_mode", "schema_name", "insert_from_clause", "select_clause", "select_clause_basic", 
		"topic_name", "table_name", "column_name", "column", "column_name_alias", 
		"column_list", "from_clause", "ignored_name", "with_ignore", "ignore_clause", 
		"pk_name", "primary_key_list", "autocreate", "autoevolve", "batch_size", 
		"batching", "capitalize", "initialize", "partition_name", "partition_list", 
		"partitionby", "distribute_name", "distribute_list", "distributeby", "timestamp_clause", 
		"timestamp_value", "timestamp_unit_clause", "timestamp_unit_value", "buckets_number", 
		"clusterby_name", "clusterby_list", "clusterby", "with_consumer_group", 
		"with_consumer_group_value", "offset_partition_inner", "offset_partition", 
		"partition_offset_list", "with_offset_list", "limit_clause", "limit_value", 
		"sample_clause", "sample_value", "sample_period", "with_unwrap_clause", 
		"with_format_clause", "with_structure", "with_format", "project_to", "version_number", 
		"storeas_clause", "storeas_type", "storeas_parameters", "storeas_parameters_tuple", 
		"storeas_parameter", "storeas_value", "with_tags", "with_key", "with_key_value", 
		"key_delimiter", "key_delimiter_value", "with_inc_mode", "inc_mode", "with_type", 
		"with_type_value", "with_doc_type", "doc_type", "with_index_suffix", "index_suffix", 
		"with_converter", "with_converter_value", "with_target", "with_target_value", 
		"with_jms_selector", "jms_selector_value", "tag_definition", "tag_key", 
		"tag_value", "ttl_clause", "ttl_type", "with_pipeline_clause", "pipeline_value", 
		"with_compression_clause", "with_compression_type", "with_delay_clause", 
		"with_delay_value"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, "'='", null, 
		"'*'", "','", "'.'", "'('", "')'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "INITIALIZE", 
		"PARTITIONBY", "DISTRIBUTEBY", "TIMESTAMP", "SYS_TIME", "WITHGROUP", "WITHOFFSET", 
		"WITHTAG", "WITHKEY", "KEYDELIM", "WITHSTRUCTURE", "WITHTYPE", "PK", "SAMPLE", 
		"EVERY", "WITHFORMAT", "WITHUNWRAP", "FORMAT", "PROJECTTO", "STOREAS", 
		"LIMIT", "INCREMENTALMODE", "WITHDOCTYPE", "WITHINDEXSUFFIX", "WITHCONVERTER", 
		"WITHJMSSELECTOR", "WITHTARGET", "WITHCOMPRESSION", "WITHDELAY", "TIMESTAMPUNIT", 
		"WITHPIPELINE", "TTL", "EQUAL", "INT", "ASTERISK", "COMMA", "DOT", "LEFT_PARAN", 
		"RIGHT_PARAN", "FIELD", "TOPICNAME", "KEYDELIMVALUE", "NEWLINE", "WS", 
		"ID"
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
			setState(194);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INSERT:
			case UPSERT:
				enterOuterAlt(_localctx, 1);
				{
				setState(192);
				insert_from_clause();
				}
				break;
			case SELECT:
				enterOuterAlt(_localctx, 2);
				{
				setState(193);
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
			setState(196);
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
			setState(198);
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
			setState(200);
			match(INSERT);
			setState(201);
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
			setState(203);
			match(UPSERT);
			setState(204);
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
			setState(206);
			match(UPSERT);
			setState(207);
			pk();
			setState(208);
			match(FIELD);
			setState(209);
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
			setState(214);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(211);
				insert_into();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(212);
				upsert_into();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(213);
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
			setState(216);
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
		public With_targetContext with_target() {
			return getRuleContext(With_targetContext.class,0);
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
		public Timestamp_unit_clauseContext timestamp_unit_clause() {
			return getRuleContext(Timestamp_unit_clauseContext.class,0);
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
		public With_keyContext with_key() {
			return getRuleContext(With_keyContext.class,0);
		}
		public Key_delimiterContext key_delimiter() {
			return getRuleContext(Key_delimiterContext.class,0);
		}
		public With_compression_clauseContext with_compression_clause() {
			return getRuleContext(With_compression_clauseContext.class,0);
		}
		public With_delay_clauseContext with_delay_clause() {
			return getRuleContext(With_delay_clauseContext.class,0);
		}
		public With_pipeline_clauseContext with_pipeline_clause() {
			return getRuleContext(With_pipeline_clauseContext.class,0);
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
			setState(218);
			write_mode();
			setState(219);
			table_name();
			setState(220);
			select_clause_basic();
			setState(222);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AUTOCREATE) {
				{
				setState(221);
				autocreate();
				}
			}

			setState(225);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHSTRUCTURE) {
				{
				setState(224);
				with_structure();
				}
			}

			setState(229);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PK) {
				{
				setState(227);
				match(PK);
				setState(228);
				primary_key_list();
				}
			}

			setState(232);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHTARGET) {
				{
				setState(231);
				with_target();
				}
			}

			setState(235);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AUTOEVOLVE) {
				{
				setState(234);
				autoevolve();
				}
			}

			setState(238);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==BATCH) {
				{
				setState(237);
				batching();
				}
			}

			setState(241);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CAPITALIZE) {
				{
				setState(240);
				capitalize();
				}
			}

			setState(244);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INITIALIZE) {
				{
				setState(243);
				initialize();
				}
			}

			setState(247);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PROJECTTO) {
				{
				setState(246);
				project_to();
				}
			}

			setState(250);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PARTITIONBY) {
				{
				setState(249);
				partitionby();
				}
			}

			setState(253);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DISTRIBUTEBY) {
				{
				setState(252);
				distributeby();
				}
			}

			setState(256);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CLUSTERBY) {
				{
				setState(255);
				clusterby();
				}
			}

			setState(259);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TIMESTAMP) {
				{
				setState(258);
				timestamp_clause();
				}
			}

			setState(262);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TIMESTAMPUNIT) {
				{
				setState(261);
				timestamp_unit_clause();
				}
			}

			setState(265);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHFORMAT) {
				{
				setState(264);
				with_format_clause();
				}
			}

			setState(268);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHUNWRAP) {
				{
				setState(267);
				with_unwrap_clause();
				}
			}

			setState(271);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STOREAS) {
				{
				setState(270);
				storeas_clause();
				}
			}

			setState(274);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHTAG) {
				{
				setState(273);
				with_tags();
				}
			}

			setState(277);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INCREMENTALMODE) {
				{
				setState(276);
				with_inc_mode();
				}
			}

			setState(280);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHTYPE) {
				{
				setState(279);
				with_type();
				}
			}

			setState(283);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHDOCTYPE) {
				{
				setState(282);
				with_doc_type();
				}
			}

			setState(286);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHINDEXSUFFIX) {
				{
				setState(285);
				with_index_suffix();
				}
			}

			setState(289);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TTL) {
				{
				setState(288);
				ttl_clause();
				}
			}

			setState(292);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHCONVERTER) {
				{
				setState(291);
				with_converter();
				}
			}

			setState(295);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHJMSSELECTOR) {
				{
				setState(294);
				with_jms_selector();
				}
			}

			setState(298);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHKEY) {
				{
				setState(297);
				with_key();
				}
			}

			setState(301);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==KEYDELIM) {
				{
				setState(300);
				key_delimiter();
				}
			}

			setState(304);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHCOMPRESSION) {
				{
				setState(303);
				with_compression_clause();
				}
			}

			setState(307);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHDELAY) {
				{
				setState(306);
				with_delay_clause();
				}
			}

			setState(310);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHPIPELINE) {
				{
				setState(309);
				with_pipeline_clause();
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
			setState(312);
			select_clause_basic();
			setState(315);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PK) {
				{
				setState(313);
				match(PK);
				setState(314);
				primary_key_list();
				}
			}

			setState(318);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHSTRUCTURE) {
				{
				setState(317);
				with_structure();
				}
			}

			setState(321);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHFORMAT) {
				{
				setState(320);
				with_format_clause();
				}
			}

			setState(324);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHUNWRAP) {
				{
				setState(323);
				with_unwrap_clause();
				}
			}

			setState(327);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHGROUP) {
				{
				setState(326);
				with_consumer_group();
				}
			}

			setState(330);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHOFFSET) {
				{
				setState(329);
				with_offset_list();
				}
			}

			setState(333);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SAMPLE) {
				{
				setState(332);
				sample_clause();
				}
			}

			setState(336);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(335);
				limit_clause();
				}
			}

			setState(339);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STOREAS) {
				{
				setState(338);
				storeas_clause();
				}
			}

			setState(342);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHTAG) {
				{
				setState(341);
				with_tags();
				}
			}

			setState(345);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INCREMENTALMODE) {
				{
				setState(344);
				with_inc_mode();
				}
			}

			setState(348);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHDOCTYPE) {
				{
				setState(347);
				with_doc_type();
				}
			}

			setState(351);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHINDEXSUFFIX) {
				{
				setState(350);
				with_index_suffix();
				}
			}

			setState(354);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHCONVERTER) {
				{
				setState(353);
				with_converter();
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
			setState(356);
			match(SELECT);
			setState(357);
			column_list();
			setState(358);
			match(FROM);
			setState(359);
			topic_name();
			setState(361);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==IGNORE) {
				{
				setState(360);
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
			setState(364); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(363);
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
				setState(366); 
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
			setState(369); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(368);
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
				setState(371); 
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
			setState(379);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(373);
				column();
				setState(376);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(374);
					match(AS);
					setState(375);
					column_name_alias();
					}
				}

				}
				break;
			case ASTERISK:
				enterOuterAlt(_localctx, 2);
				{
				setState(378);
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
			setState(381);
			match(FIELD);
			setState(386);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,51,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(382);
					match(DOT);
					setState(383);
					match(FIELD);
					}
					} 
				}
				setState(388);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,51,_ctx);
			}
			setState(391);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DOT) {
				{
				setState(389);
				match(DOT);
				setState(390);
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
			setState(393);
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
			setState(395);
			column_name();
			setState(400);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(396);
				match(COMMA);
				setState(397);
				column_name();
				}
				}
				setState(402);
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
			setState(403);
			match(FROM);
			setState(404);
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
			setState(406);
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
			setState(408);
			match(IGNORE);
			setState(409);
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
			setState(411);
			column_name();
			setState(416);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(412);
				match(COMMA);
				setState(413);
				column_name();
				}
				}
				setState(418);
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
			setState(419);
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
			setState(421);
			pk_name();
			setState(426);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(422);
				match(COMMA);
				setState(423);
				pk_name();
				}
				}
				setState(428);
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
			setState(429);
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
			setState(431);
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
			setState(433);
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
			setState(435);
			match(BATCH);
			setState(436);
			match(EQUAL);
			setState(437);
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
			setState(439);
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
			setState(441);
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
			setState(443);
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
			setState(445);
			partition_name();
			setState(450);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(446);
				match(COMMA);
				setState(447);
				partition_name();
				}
				}
				setState(452);
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
			setState(453);
			match(PARTITIONBY);
			setState(454);
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
			setState(456);
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
			setState(458);
			distribute_name();
			setState(463);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(459);
				match(COMMA);
				setState(460);
				distribute_name();
				}
				}
				setState(465);
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
			setState(466);
			match(DISTRIBUTEBY);
			setState(467);
			distribute_list();
			setState(468);
			match(INTO);
			setState(469);
			buckets_number();
			setState(470);
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
			setState(472);
			match(TIMESTAMP);
			setState(473);
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
			setState(475);
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

	public static class Timestamp_unit_clauseContext extends ParserRuleContext {
		public TerminalNode TIMESTAMPUNIT() { return getToken(ConnectorParser.TIMESTAMPUNIT, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public Timestamp_unit_valueContext timestamp_unit_value() {
			return getRuleContext(Timestamp_unit_valueContext.class,0);
		}
		public Timestamp_unit_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timestamp_unit_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterTimestamp_unit_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitTimestamp_unit_clause(this);
		}
	}

	public final Timestamp_unit_clauseContext timestamp_unit_clause() throws RecognitionException {
		Timestamp_unit_clauseContext _localctx = new Timestamp_unit_clauseContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_timestamp_unit_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(477);
			match(TIMESTAMPUNIT);
			setState(478);
			match(EQUAL);
			setState(479);
			timestamp_unit_value();
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

	public static class Timestamp_unit_valueContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public Timestamp_unit_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timestamp_unit_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterTimestamp_unit_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitTimestamp_unit_value(this);
		}
	}

	public final Timestamp_unit_valueContext timestamp_unit_value() throws RecognitionException {
		Timestamp_unit_valueContext _localctx = new Timestamp_unit_valueContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_timestamp_unit_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(481);
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
		enterRule(_localctx, 78, RULE_buckets_number);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(483);
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
		enterRule(_localctx, 80, RULE_clusterby_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(485);
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
		enterRule(_localctx, 82, RULE_clusterby_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(487);
			clusterby_name();
			setState(492);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(488);
				match(COMMA);
				setState(489);
				clusterby_name();
				}
				}
				setState(494);
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
		enterRule(_localctx, 84, RULE_clusterby);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(495);
			match(CLUSTERBY);
			setState(496);
			clusterby_list();
			setState(497);
			match(INTO);
			setState(498);
			buckets_number();
			setState(499);
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
		enterRule(_localctx, 86, RULE_with_consumer_group);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(501);
			match(WITHGROUP);
			setState(502);
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
		enterRule(_localctx, 88, RULE_with_consumer_group_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(504);
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
		enterRule(_localctx, 90, RULE_offset_partition_inner);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(506);
			match(INT);
			setState(509);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(507);
				match(COMMA);
				setState(508);
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
		enterRule(_localctx, 92, RULE_offset_partition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(511);
			match(LEFT_PARAN);
			setState(512);
			offset_partition_inner();
			setState(513);
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
		enterRule(_localctx, 94, RULE_partition_offset_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(515);
			offset_partition();
			setState(520);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(516);
				match(COMMA);
				setState(517);
				offset_partition();
				}
				}
				setState(522);
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
		enterRule(_localctx, 96, RULE_with_offset_list);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(523);
			match(WITHOFFSET);
			setState(524);
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
		enterRule(_localctx, 98, RULE_limit_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(526);
			match(LIMIT);
			setState(527);
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
		enterRule(_localctx, 100, RULE_limit_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(529);
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
		enterRule(_localctx, 102, RULE_sample_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(531);
			match(SAMPLE);
			setState(532);
			sample_value();
			setState(533);
			match(EVERY);
			setState(534);
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
		enterRule(_localctx, 104, RULE_sample_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(536);
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
		enterRule(_localctx, 106, RULE_sample_period);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(538);
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
		enterRule(_localctx, 108, RULE_with_unwrap_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(540);
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
		enterRule(_localctx, 110, RULE_with_format_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(542);
			match(WITHFORMAT);
			setState(543);
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
		enterRule(_localctx, 112, RULE_with_structure);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(545);
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
		enterRule(_localctx, 114, RULE_with_format);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(547);
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
		enterRule(_localctx, 116, RULE_project_to);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(549);
			match(PROJECTTO);
			setState(550);
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
		enterRule(_localctx, 118, RULE_version_number);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(552);
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
		enterRule(_localctx, 120, RULE_storeas_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(554);
			match(STOREAS);
			setState(555);
			storeas_type();
			setState(559);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==LEFT_PARAN) {
				{
				{
				setState(556);
				storeas_parameters();
				}
				}
				setState(561);
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
		enterRule(_localctx, 122, RULE_storeas_type);
		int _la;
		try {
			setState(568);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(562);
				match(FIELD);
				}
				break;
			case DOT:
			case TOPICNAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(564); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(563);
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
					setState(566); 
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
		enterRule(_localctx, 124, RULE_storeas_parameters);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(570);
			match(LEFT_PARAN);
			setState(571);
			storeas_parameters_tuple();
			setState(576);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(572);
				match(COMMA);
				setState(573);
				storeas_parameters_tuple();
				}
				}
				setState(578);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(579);
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
		enterRule(_localctx, 126, RULE_storeas_parameters_tuple);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(581);
			storeas_parameter();
			setState(582);
			match(EQUAL);
			setState(583);
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
		enterRule(_localctx, 128, RULE_storeas_parameter);
		int _la;
		try {
			setState(591);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(585);
				match(FIELD);
				}
				break;
			case DOT:
			case TOPICNAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(587); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(586);
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
					setState(589); 
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
		enterRule(_localctx, 130, RULE_storeas_value);
		int _la;
		try {
			setState(600);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(593);
				match(FIELD);
				}
				break;
			case DOT:
			case TOPICNAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(595); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(594);
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
					setState(597); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==TOPICNAME );
				}
				break;
			case INT:
				enterOuterAlt(_localctx, 3);
				{
				setState(599);
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
		enterRule(_localctx, 132, RULE_with_tags);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(602);
			match(WITHTAG);
			{
			setState(603);
			match(LEFT_PARAN);
			setState(604);
			tag_definition();
			setState(609);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(605);
				match(COMMA);
				setState(606);
				tag_definition();
				}
				}
				setState(611);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(612);
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

	public static class With_keyContext extends ParserRuleContext {
		public TerminalNode WITHKEY() { return getToken(ConnectorParser.WITHKEY, 0); }
		public TerminalNode LEFT_PARAN() { return getToken(ConnectorParser.LEFT_PARAN, 0); }
		public List<With_key_valueContext> with_key_value() {
			return getRuleContexts(With_key_valueContext.class);
		}
		public With_key_valueContext with_key_value(int i) {
			return getRuleContext(With_key_valueContext.class,i);
		}
		public TerminalNode RIGHT_PARAN() { return getToken(ConnectorParser.RIGHT_PARAN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(ConnectorParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(ConnectorParser.COMMA, i);
		}
		public With_keyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_key; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_key(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_key(this);
		}
	}

	public final With_keyContext with_key() throws RecognitionException {
		With_keyContext _localctx = new With_keyContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_with_key);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(614);
			match(WITHKEY);
			{
			setState(615);
			match(LEFT_PARAN);
			setState(616);
			with_key_value();
			setState(621);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(617);
				match(COMMA);
				setState(618);
				with_key_value();
				}
				}
				setState(623);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(624);
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

	public static class With_key_valueContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public TerminalNode TOPICNAME() { return getToken(ConnectorParser.TOPICNAME, 0); }
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public With_key_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_key_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_key_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_key_value(this);
		}
	}

	public final With_key_valueContext with_key_value() throws RecognitionException {
		With_key_valueContext _localctx = new With_key_valueContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_with_key_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(626);
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

	public static class Key_delimiterContext extends ParserRuleContext {
		public TerminalNode KEYDELIM() { return getToken(ConnectorParser.KEYDELIM, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public Key_delimiter_valueContext key_delimiter_value() {
			return getRuleContext(Key_delimiter_valueContext.class,0);
		}
		public Key_delimiterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_key_delimiter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterKey_delimiter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitKey_delimiter(this);
		}
	}

	public final Key_delimiterContext key_delimiter() throws RecognitionException {
		Key_delimiterContext _localctx = new Key_delimiterContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_key_delimiter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(628);
			match(KEYDELIM);
			setState(629);
			match(EQUAL);
			setState(630);
			key_delimiter_value();
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

	public static class Key_delimiter_valueContext extends ParserRuleContext {
		public TerminalNode KEYDELIMVALUE() { return getToken(ConnectorParser.KEYDELIMVALUE, 0); }
		public Key_delimiter_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_key_delimiter_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterKey_delimiter_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitKey_delimiter_value(this);
		}
	}

	public final Key_delimiter_valueContext key_delimiter_value() throws RecognitionException {
		Key_delimiter_valueContext _localctx = new Key_delimiter_valueContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_key_delimiter_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(632);
			match(KEYDELIMVALUE);
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
		enterRule(_localctx, 142, RULE_with_inc_mode);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(634);
			match(INCREMENTALMODE);
			setState(635);
			match(EQUAL);
			setState(636);
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
		enterRule(_localctx, 144, RULE_inc_mode);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(638);
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
		enterRule(_localctx, 146, RULE_with_type);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(640);
			match(WITHTYPE);
			setState(641);
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
		enterRule(_localctx, 148, RULE_with_type_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(643);
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
		enterRule(_localctx, 150, RULE_with_doc_type);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(645);
			match(WITHDOCTYPE);
			setState(646);
			match(EQUAL);
			setState(647);
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
		enterRule(_localctx, 152, RULE_doc_type);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(649);
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
		enterRule(_localctx, 154, RULE_with_index_suffix);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(651);
			match(WITHINDEXSUFFIX);
			setState(652);
			match(EQUAL);
			setState(653);
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
		enterRule(_localctx, 156, RULE_index_suffix);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(655);
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
		enterRule(_localctx, 158, RULE_with_converter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(657);
			match(WITHCONVERTER);
			setState(658);
			match(EQUAL);
			setState(659);
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
		enterRule(_localctx, 160, RULE_with_converter_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(661);
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

	public static class With_targetContext extends ParserRuleContext {
		public TerminalNode WITHTARGET() { return getToken(ConnectorParser.WITHTARGET, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public With_target_valueContext with_target_value() {
			return getRuleContext(With_target_valueContext.class,0);
		}
		public With_targetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_target; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_target(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_target(this);
		}
	}

	public final With_targetContext with_target() throws RecognitionException {
		With_targetContext _localctx = new With_targetContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_with_target);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(663);
			match(WITHTARGET);
			setState(664);
			match(EQUAL);
			setState(665);
			with_target_value();
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

	public static class With_target_valueContext extends ParserRuleContext {
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
		public With_target_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_target_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_target_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_target_value(this);
		}
	}

	public final With_target_valueContext with_target_value() throws RecognitionException {
		With_target_valueContext _localctx = new With_target_valueContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_with_target_value);
		int _la;
		try {
			setState(677);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(668); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(667);
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
					setState(670); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==FIELD );
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(673); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(672);
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
					setState(675); 
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
		enterRule(_localctx, 166, RULE_with_jms_selector);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(679);
			match(WITHJMSSELECTOR);
			setState(680);
			match(EQUAL);
			setState(681);
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
		enterRule(_localctx, 168, RULE_jms_selector_value);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(683);
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
		public TerminalNode AS() { return getToken(ConnectorParser.AS, 0); }
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
		enterRule(_localctx, 170, RULE_tag_definition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(685);
			tag_key();
			setState(690);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case EQUAL:
				{
				{
				setState(686);
				match(EQUAL);
				setState(687);
				tag_value();
				}
				}
				break;
			case AS:
				{
				{
				setState(688);
				match(AS);
				setState(689);
				tag_value();
				}
				}
				break;
			case COMMA:
			case RIGHT_PARAN:
				break;
			default:
				break;
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
		enterRule(_localctx, 172, RULE_tag_key);
		int _la;
		try {
			setState(702);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,77,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(693); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(692);
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
					setState(695); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==FIELD );
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(698); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(697);
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
					setState(700); 
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
		enterRule(_localctx, 174, RULE_tag_value);
		int _la;
		try {
			setState(711);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(704);
				match(FIELD);
				}
				break;
			case DOT:
			case TOPICNAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(706); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(705);
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
					setState(708); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==TOPICNAME );
				}
				break;
			case INT:
				enterOuterAlt(_localctx, 3);
				{
				setState(710);
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
		enterRule(_localctx, 176, RULE_ttl_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(713);
			match(TTL);
			setState(714);
			match(EQUAL);
			setState(715);
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
		enterRule(_localctx, 178, RULE_ttl_type);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(717);
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

	public static class With_pipeline_clauseContext extends ParserRuleContext {
		public TerminalNode WITHPIPELINE() { return getToken(ConnectorParser.WITHPIPELINE, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public Pipeline_valueContext pipeline_value() {
			return getRuleContext(Pipeline_valueContext.class,0);
		}
		public With_pipeline_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_pipeline_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_pipeline_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_pipeline_clause(this);
		}
	}

	public final With_pipeline_clauseContext with_pipeline_clause() throws RecognitionException {
		With_pipeline_clauseContext _localctx = new With_pipeline_clauseContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_with_pipeline_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(719);
			match(WITHPIPELINE);
			setState(720);
			match(EQUAL);
			setState(721);
			pipeline_value();
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

	public static class Pipeline_valueContext extends ParserRuleContext {
		public List<TerminalNode> FIELD() { return getTokens(ConnectorParser.FIELD); }
		public TerminalNode FIELD(int i) {
			return getToken(ConnectorParser.FIELD, i);
		}
		public List<TerminalNode> INT() { return getTokens(ConnectorParser.INT); }
		public TerminalNode INT(int i) {
			return getToken(ConnectorParser.INT, i);
		}
		public List<TerminalNode> DOT() { return getTokens(ConnectorParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(ConnectorParser.DOT, i);
		}
		public List<TerminalNode> TOPICNAME() { return getTokens(ConnectorParser.TOPICNAME); }
		public TerminalNode TOPICNAME(int i) {
			return getToken(ConnectorParser.TOPICNAME, i);
		}
		public Pipeline_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pipeline_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterPipeline_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitPipeline_value(this);
		}
	}

	public final Pipeline_valueContext pipeline_value() throws RecognitionException {
		Pipeline_valueContext _localctx = new Pipeline_valueContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_pipeline_value);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(730); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				setState(730);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case FIELD:
					{
					setState(723);
					match(FIELD);
					}
					break;
				case DOT:
				case TOPICNAME:
					{
					setState(725); 
					_errHandler.sync(this);
					_alt = 1;
					do {
						switch (_alt) {
						case 1:
							{
							{
							setState(724);
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
							break;
						default:
							throw new NoViableAltException(this);
						}
						setState(727); 
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,80,_ctx);
					} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
					}
					break;
				case INT:
					{
					setState(729);
					match(INT);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(732); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( (((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << INT) | (1L << DOT) | (1L << FIELD) | (1L << TOPICNAME))) != 0) );
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

	public static class With_compression_clauseContext extends ParserRuleContext {
		public TerminalNode WITHCOMPRESSION() { return getToken(ConnectorParser.WITHCOMPRESSION, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public With_compression_typeContext with_compression_type() {
			return getRuleContext(With_compression_typeContext.class,0);
		}
		public With_compression_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_compression_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_compression_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_compression_clause(this);
		}
	}

	public final With_compression_clauseContext with_compression_clause() throws RecognitionException {
		With_compression_clauseContext _localctx = new With_compression_clauseContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_with_compression_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(734);
			match(WITHCOMPRESSION);
			setState(735);
			match(EQUAL);
			setState(736);
			with_compression_type();
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

	public static class With_compression_typeContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public With_compression_typeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_compression_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_compression_type(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_compression_type(this);
		}
	}

	public final With_compression_typeContext with_compression_type() throws RecognitionException {
		With_compression_typeContext _localctx = new With_compression_typeContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_with_compression_type);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(738);
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

	public static class With_delay_clauseContext extends ParserRuleContext {
		public TerminalNode WITHDELAY() { return getToken(ConnectorParser.WITHDELAY, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public With_delay_valueContext with_delay_value() {
			return getRuleContext(With_delay_valueContext.class,0);
		}
		public With_delay_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_delay_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_delay_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_delay_clause(this);
		}
	}

	public final With_delay_clauseContext with_delay_clause() throws RecognitionException {
		With_delay_clauseContext _localctx = new With_delay_clauseContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_with_delay_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(740);
			match(WITHDELAY);
			setState(741);
			match(EQUAL);
			setState(742);
			with_delay_value();
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

	public static class With_delay_valueContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(ConnectorParser.INT, 0); }
		public With_delay_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_delay_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_delay_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_delay_value(this);
		}
	}

	public final With_delay_valueContext with_delay_value() throws RecognitionException {
		With_delay_valueContext _localctx = new With_delay_valueContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_with_delay_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(744);
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
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3<\u02ed\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
		"\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4_\t_\4"+
		"`\t`\4a\ta\3\2\3\2\5\2\u00c5\n\2\3\3\3\3\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3"+
		"\6\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\5\b\u00d9\n\b\3\t\3\t\3\n\3\n\3\n\3"+
		"\n\5\n\u00e1\n\n\3\n\5\n\u00e4\n\n\3\n\3\n\5\n\u00e8\n\n\3\n\5\n\u00eb"+
		"\n\n\3\n\5\n\u00ee\n\n\3\n\5\n\u00f1\n\n\3\n\5\n\u00f4\n\n\3\n\5\n\u00f7"+
		"\n\n\3\n\5\n\u00fa\n\n\3\n\5\n\u00fd\n\n\3\n\5\n\u0100\n\n\3\n\5\n\u0103"+
		"\n\n\3\n\5\n\u0106\n\n\3\n\5\n\u0109\n\n\3\n\5\n\u010c\n\n\3\n\5\n\u010f"+
		"\n\n\3\n\5\n\u0112\n\n\3\n\5\n\u0115\n\n\3\n\5\n\u0118\n\n\3\n\5\n\u011b"+
		"\n\n\3\n\5\n\u011e\n\n\3\n\5\n\u0121\n\n\3\n\5\n\u0124\n\n\3\n\5\n\u0127"+
		"\n\n\3\n\5\n\u012a\n\n\3\n\5\n\u012d\n\n\3\n\5\n\u0130\n\n\3\n\5\n\u0133"+
		"\n\n\3\n\5\n\u0136\n\n\3\n\5\n\u0139\n\n\3\13\3\13\3\13\5\13\u013e\n\13"+
		"\3\13\5\13\u0141\n\13\3\13\5\13\u0144\n\13\3\13\5\13\u0147\n\13\3\13\5"+
		"\13\u014a\n\13\3\13\5\13\u014d\n\13\3\13\5\13\u0150\n\13\3\13\5\13\u0153"+
		"\n\13\3\13\5\13\u0156\n\13\3\13\5\13\u0159\n\13\3\13\5\13\u015c\n\13\3"+
		"\13\5\13\u015f\n\13\3\13\5\13\u0162\n\13\3\13\5\13\u0165\n\13\3\f\3\f"+
		"\3\f\3\f\3\f\5\f\u016c\n\f\3\r\6\r\u016f\n\r\r\r\16\r\u0170\3\16\6\16"+
		"\u0174\n\16\r\16\16\16\u0175\3\17\3\17\3\17\5\17\u017b\n\17\3\17\5\17"+
		"\u017e\n\17\3\20\3\20\3\20\7\20\u0183\n\20\f\20\16\20\u0186\13\20\3\20"+
		"\3\20\5\20\u018a\n\20\3\21\3\21\3\22\3\22\3\22\7\22\u0191\n\22\f\22\16"+
		"\22\u0194\13\22\3\23\3\23\3\23\3\24\3\24\3\25\3\25\3\25\3\26\3\26\3\26"+
		"\7\26\u01a1\n\26\f\26\16\26\u01a4\13\26\3\27\3\27\3\30\3\30\3\30\7\30"+
		"\u01ab\n\30\f\30\16\30\u01ae\13\30\3\31\3\31\3\32\3\32\3\33\3\33\3\34"+
		"\3\34\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3 \7 \u01c3\n \f "+
		"\16 \u01c6\13 \3!\3!\3!\3\"\3\"\3#\3#\3#\7#\u01d0\n#\f#\16#\u01d3\13#"+
		"\3$\3$\3$\3$\3$\3$\3%\3%\3%\3&\3&\3\'\3\'\3\'\3\'\3(\3(\3)\3)\3*\3*\3"+
		"+\3+\3+\7+\u01ed\n+\f+\16+\u01f0\13+\3,\3,\3,\3,\3,\3,\3-\3-\3-\3.\3."+
		"\3/\3/\3/\5/\u0200\n/\3\60\3\60\3\60\3\60\3\61\3\61\3\61\7\61\u0209\n"+
		"\61\f\61\16\61\u020c\13\61\3\62\3\62\3\62\3\63\3\63\3\63\3\64\3\64\3\65"+
		"\3\65\3\65\3\65\3\65\3\66\3\66\3\67\3\67\38\38\39\39\39\3:\3:\3;\3;\3"+
		"<\3<\3<\3=\3=\3>\3>\3>\7>\u0230\n>\f>\16>\u0233\13>\3?\3?\6?\u0237\n?"+
		"\r?\16?\u0238\5?\u023b\n?\3@\3@\3@\3@\7@\u0241\n@\f@\16@\u0244\13@\3@"+
		"\3@\3A\3A\3A\3A\3B\3B\6B\u024e\nB\rB\16B\u024f\5B\u0252\nB\3C\3C\6C\u0256"+
		"\nC\rC\16C\u0257\3C\5C\u025b\nC\3D\3D\3D\3D\3D\7D\u0262\nD\fD\16D\u0265"+
		"\13D\3D\3D\3E\3E\3E\3E\3E\7E\u026e\nE\fE\16E\u0271\13E\3E\3E\3F\3F\3G"+
		"\3G\3G\3G\3H\3H\3I\3I\3I\3I\3J\3J\3K\3K\3K\3L\3L\3M\3M\3M\3M\3N\3N\3O"+
		"\3O\3O\3O\3P\3P\3Q\3Q\3Q\3Q\3R\3R\3S\3S\3S\3S\3T\6T\u029f\nT\rT\16T\u02a0"+
		"\3T\6T\u02a4\nT\rT\16T\u02a5\5T\u02a8\nT\3U\3U\3U\3U\3V\3V\3W\3W\3W\3"+
		"W\3W\5W\u02b5\nW\3X\6X\u02b8\nX\rX\16X\u02b9\3X\6X\u02bd\nX\rX\16X\u02be"+
		"\5X\u02c1\nX\3Y\3Y\6Y\u02c5\nY\rY\16Y\u02c6\3Y\5Y\u02ca\nY\3Z\3Z\3Z\3"+
		"Z\3[\3[\3\\\3\\\3\\\3\\\3]\3]\6]\u02d8\n]\r]\16]\u02d9\3]\6]\u02dd\n]"+
		"\r]\16]\u02de\3^\3^\3^\3^\3_\3_\3`\3`\3`\3`\3a\3a\3a\2\2b\2\4\6\b\n\f"+
		"\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^"+
		"`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090"+
		"\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8"+
		"\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be\u00c0"+
		"\2\t\4\2\64\64\678\3\2\678\4\2\24\24\67\67\4\2\61\61\678\4\2\64\6488\4"+
		"\288<<\4\2\64\64\67\67\2\u02e4\2\u00c4\3\2\2\2\4\u00c6\3\2\2\2\6\u00c8"+
		"\3\2\2\2\b\u00ca\3\2\2\2\n\u00cd\3\2\2\2\f\u00d0\3\2\2\2\16\u00d8\3\2"+
		"\2\2\20\u00da\3\2\2\2\22\u00dc\3\2\2\2\24\u013a\3\2\2\2\26\u0166\3\2\2"+
		"\2\30\u016e\3\2\2\2\32\u0173\3\2\2\2\34\u017d\3\2\2\2\36\u017f\3\2\2\2"+
		" \u018b\3\2\2\2\"\u018d\3\2\2\2$\u0195\3\2\2\2&\u0198\3\2\2\2(\u019a\3"+
		"\2\2\2*\u019d\3\2\2\2,\u01a5\3\2\2\2.\u01a7\3\2\2\2\60\u01af\3\2\2\2\62"+
		"\u01b1\3\2\2\2\64\u01b3\3\2\2\2\66\u01b5\3\2\2\28\u01b9\3\2\2\2:\u01bb"+
		"\3\2\2\2<\u01bd\3\2\2\2>\u01bf\3\2\2\2@\u01c7\3\2\2\2B\u01ca\3\2\2\2D"+
		"\u01cc\3\2\2\2F\u01d4\3\2\2\2H\u01da\3\2\2\2J\u01dd\3\2\2\2L\u01df\3\2"+
		"\2\2N\u01e3\3\2\2\2P\u01e5\3\2\2\2R\u01e7\3\2\2\2T\u01e9\3\2\2\2V\u01f1"+
		"\3\2\2\2X\u01f7\3\2\2\2Z\u01fa\3\2\2\2\\\u01fc\3\2\2\2^\u0201\3\2\2\2"+
		"`\u0205\3\2\2\2b\u020d\3\2\2\2d\u0210\3\2\2\2f\u0213\3\2\2\2h\u0215\3"+
		"\2\2\2j\u021a\3\2\2\2l\u021c\3\2\2\2n\u021e\3\2\2\2p\u0220\3\2\2\2r\u0223"+
		"\3\2\2\2t\u0225\3\2\2\2v\u0227\3\2\2\2x\u022a\3\2\2\2z\u022c\3\2\2\2|"+
		"\u023a\3\2\2\2~\u023c\3\2\2\2\u0080\u0247\3\2\2\2\u0082\u0251\3\2\2\2"+
		"\u0084\u025a\3\2\2\2\u0086\u025c\3\2\2\2\u0088\u0268\3\2\2\2\u008a\u0274"+
		"\3\2\2\2\u008c\u0276\3\2\2\2\u008e\u027a\3\2\2\2\u0090\u027c\3\2\2\2\u0092"+
		"\u0280\3\2\2\2\u0094\u0282\3\2\2\2\u0096\u0285\3\2\2\2\u0098\u0287\3\2"+
		"\2\2\u009a\u028b\3\2\2\2\u009c\u028d\3\2\2\2\u009e\u0291\3\2\2\2\u00a0"+
		"\u0293\3\2\2\2\u00a2\u0297\3\2\2\2\u00a4\u0299\3\2\2\2\u00a6\u02a7\3\2"+
		"\2\2\u00a8\u02a9\3\2\2\2\u00aa\u02ad\3\2\2\2\u00ac\u02af\3\2\2\2\u00ae"+
		"\u02c0\3\2\2\2\u00b0\u02c9\3\2\2\2\u00b2\u02cb\3\2\2\2\u00b4\u02cf\3\2"+
		"\2\2\u00b6\u02d1\3\2\2\2\u00b8\u02dc\3\2\2\2\u00ba\u02e0\3\2\2\2\u00bc"+
		"\u02e4\3\2\2\2\u00be\u02e6\3\2\2\2\u00c0\u02ea\3\2\2\2\u00c2\u00c5\5\22"+
		"\n\2\u00c3\u00c5\5\24\13\2\u00c4\u00c2\3\2\2\2\u00c4\u00c3\3\2\2\2\u00c5"+
		"\3\3\2\2\2\u00c6\u00c7\7\5\2\2\u00c7\5\3\2\2\2\u00c8\u00c9\7\34\2\2\u00c9"+
		"\7\3\2\2\2\u00ca\u00cb\7\3\2\2\u00cb\u00cc\5\4\3\2\u00cc\t\3\2\2\2\u00cd"+
		"\u00ce\7\4\2\2\u00ce\u00cf\5\4\3\2\u00cf\13\3\2\2\2\u00d0\u00d1\7\4\2"+
		"\2\u00d1\u00d2\5\6\4\2\u00d2\u00d3\7\67\2\2\u00d3\u00d4\5\4\3\2\u00d4"+
		"\r\3\2\2\2\u00d5\u00d9\5\b\5\2\u00d6\u00d9\5\n\6\2\u00d7\u00d9\5\f\7\2"+
		"\u00d8\u00d5\3\2\2\2\u00d8\u00d6\3\2\2\2\u00d8\u00d7\3\2\2\2\u00d9\17"+
		"\3\2\2\2\u00da\u00db\7\67\2\2\u00db\21\3\2\2\2\u00dc\u00dd\5\16\b\2\u00dd"+
		"\u00de\5\32\16\2\u00de\u00e0\5\26\f\2\u00df\u00e1\5\60\31\2\u00e0\u00df"+
		"\3\2\2\2\u00e0\u00e1\3\2\2\2\u00e1\u00e3\3\2\2\2\u00e2\u00e4\5r:\2\u00e3"+
		"\u00e2\3\2\2\2\u00e3\u00e4\3\2\2\2\u00e4\u00e7\3\2\2\2\u00e5\u00e6\7\34"+
		"\2\2\u00e6\u00e8\5.\30\2\u00e7\u00e5\3\2\2\2\u00e7\u00e8\3\2\2\2\u00e8"+
		"\u00ea\3\2\2\2\u00e9\u00eb\5\u00a4S\2\u00ea\u00e9\3\2\2\2\u00ea\u00eb"+
		"\3\2\2\2\u00eb\u00ed\3\2\2\2\u00ec\u00ee\5\62\32\2\u00ed\u00ec\3\2\2\2"+
		"\u00ed\u00ee\3\2\2\2\u00ee\u00f0\3\2\2\2\u00ef\u00f1\5\66\34\2\u00f0\u00ef"+
		"\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1\u00f3\3\2\2\2\u00f2\u00f4\58\35\2\u00f3"+
		"\u00f2\3\2\2\2\u00f3\u00f4\3\2\2\2\u00f4\u00f6\3\2\2\2\u00f5\u00f7\5:"+
		"\36\2\u00f6\u00f5\3\2\2\2\u00f6\u00f7\3\2\2\2\u00f7\u00f9\3\2\2\2\u00f8"+
		"\u00fa\5v<\2\u00f9\u00f8\3\2\2\2\u00f9\u00fa\3\2\2\2\u00fa\u00fc\3\2\2"+
		"\2\u00fb\u00fd\5@!\2\u00fc\u00fb\3\2\2\2\u00fc\u00fd\3\2\2\2\u00fd\u00ff"+
		"\3\2\2\2\u00fe\u0100\5F$\2\u00ff\u00fe\3\2\2\2\u00ff\u0100\3\2\2\2\u0100"+
		"\u0102\3\2\2\2\u0101\u0103\5V,\2\u0102\u0101\3\2\2\2\u0102\u0103\3\2\2"+
		"\2\u0103\u0105\3\2\2\2\u0104\u0106\5H%\2\u0105\u0104\3\2\2\2\u0105\u0106"+
		"\3\2\2\2\u0106\u0108\3\2\2\2\u0107\u0109\5L\'\2\u0108\u0107\3\2\2\2\u0108"+
		"\u0109\3\2\2\2\u0109\u010b\3\2\2\2\u010a\u010c\5p9\2\u010b\u010a\3\2\2"+
		"\2\u010b\u010c\3\2\2\2\u010c\u010e\3\2\2\2\u010d\u010f\5n8\2\u010e\u010d"+
		"\3\2\2\2\u010e\u010f\3\2\2\2\u010f\u0111\3\2\2\2\u0110\u0112\5z>\2\u0111"+
		"\u0110\3\2\2\2\u0111\u0112\3\2\2\2\u0112\u0114\3\2\2\2\u0113\u0115\5\u0086"+
		"D\2\u0114\u0113\3\2\2\2\u0114\u0115\3\2\2\2\u0115\u0117\3\2\2\2\u0116"+
		"\u0118\5\u0090I\2\u0117\u0116\3\2\2\2\u0117\u0118\3\2\2\2\u0118\u011a"+
		"\3\2\2\2\u0119\u011b\5\u0094K\2\u011a\u0119\3\2\2\2\u011a\u011b\3\2\2"+
		"\2\u011b\u011d\3\2\2\2\u011c\u011e\5\u0098M\2\u011d\u011c\3\2\2\2\u011d"+
		"\u011e\3\2\2\2\u011e\u0120\3\2\2\2\u011f\u0121\5\u009cO\2\u0120\u011f"+
		"\3\2\2\2\u0120\u0121\3\2\2\2\u0121\u0123\3\2\2\2\u0122\u0124\5\u00b2Z"+
		"\2\u0123\u0122\3\2\2\2\u0123\u0124\3\2\2\2\u0124\u0126\3\2\2\2\u0125\u0127"+
		"\5\u00a0Q\2\u0126\u0125\3\2\2\2\u0126\u0127\3\2\2\2\u0127\u0129\3\2\2"+
		"\2\u0128\u012a\5\u00a8U\2\u0129\u0128\3\2\2\2\u0129\u012a\3\2\2\2\u012a"+
		"\u012c\3\2\2\2\u012b\u012d\5\u0088E\2\u012c\u012b\3\2\2\2\u012c\u012d"+
		"\3\2\2\2\u012d\u012f\3\2\2\2\u012e\u0130\5\u008cG\2\u012f\u012e\3\2\2"+
		"\2\u012f\u0130\3\2\2\2\u0130\u0132\3\2\2\2\u0131\u0133\5\u00ba^\2\u0132"+
		"\u0131\3\2\2\2\u0132\u0133\3\2\2\2\u0133\u0135\3\2\2\2\u0134\u0136\5\u00be"+
		"`\2\u0135\u0134\3\2\2\2\u0135\u0136\3\2\2\2\u0136\u0138\3\2\2\2\u0137"+
		"\u0139\5\u00b6\\\2\u0138\u0137\3\2\2\2\u0138\u0139\3\2\2\2\u0139\23\3"+
		"\2\2\2\u013a\u013d\5\26\f\2\u013b\u013c\7\34\2\2\u013c\u013e\5.\30\2\u013d"+
		"\u013b\3\2\2\2\u013d\u013e\3\2\2\2\u013e\u0140\3\2\2\2\u013f\u0141\5r"+
		":\2\u0140\u013f\3\2\2\2\u0140\u0141\3\2\2\2\u0141\u0143\3\2\2\2\u0142"+
		"\u0144\5p9\2\u0143\u0142\3\2\2\2\u0143\u0144\3\2\2\2\u0144\u0146\3\2\2"+
		"\2\u0145\u0147\5n8\2\u0146\u0145\3\2\2\2\u0146\u0147\3\2\2\2\u0147\u0149"+
		"\3\2\2\2\u0148\u014a\5X-\2\u0149\u0148\3\2\2\2\u0149\u014a\3\2\2\2\u014a"+
		"\u014c\3\2\2\2\u014b\u014d\5b\62\2\u014c\u014b\3\2\2\2\u014c\u014d\3\2"+
		"\2\2\u014d\u014f\3\2\2\2\u014e\u0150\5h\65\2\u014f\u014e\3\2\2\2\u014f"+
		"\u0150\3\2\2\2\u0150\u0152\3\2\2\2\u0151\u0153\5d\63\2\u0152\u0151\3\2"+
		"\2\2\u0152\u0153\3\2\2\2\u0153\u0155\3\2\2\2\u0154\u0156\5z>\2\u0155\u0154"+
		"\3\2\2\2\u0155\u0156\3\2\2\2\u0156\u0158\3\2\2\2\u0157\u0159\5\u0086D"+
		"\2\u0158\u0157\3\2\2\2\u0158\u0159\3\2\2\2\u0159\u015b\3\2\2\2\u015a\u015c"+
		"\5\u0090I\2\u015b\u015a\3\2\2\2\u015b\u015c\3\2\2\2\u015c\u015e\3\2\2"+
		"\2\u015d\u015f\5\u0098M\2\u015e\u015d\3\2\2\2\u015e\u015f\3\2\2\2\u015f"+
		"\u0161\3\2\2\2\u0160\u0162\5\u009cO\2\u0161\u0160\3\2\2\2\u0161\u0162"+
		"\3\2\2\2\u0162\u0164\3\2\2\2\u0163\u0165\5\u00a0Q\2\u0164\u0163\3\2\2"+
		"\2\u0164\u0165\3\2\2\2\u0165\25\3\2\2\2\u0166\u0167\7\6\2\2\u0167\u0168"+
		"\5\"\22\2\u0168\u0169\7\7\2\2\u0169\u016b\5\30\r\2\u016a\u016c\5(\25\2"+
		"\u016b\u016a\3\2\2\2\u016b\u016c\3\2\2\2\u016c\27\3\2\2\2\u016d\u016f"+
		"\t\2\2\2\u016e\u016d\3\2\2\2\u016f\u0170\3\2\2\2\u0170\u016e\3\2\2\2\u0170"+
		"\u0171\3\2\2\2\u0171\31\3\2\2\2\u0172\u0174\t\2\2\2\u0173\u0172\3\2\2"+
		"\2\u0174\u0175\3\2\2\2\u0175\u0173\3\2\2\2\u0175\u0176\3\2\2\2\u0176\33"+
		"\3\2\2\2\u0177\u017a\5\36\20\2\u0178\u0179\7\t\2\2\u0179\u017b\5 \21\2"+
		"\u017a\u0178\3\2\2\2\u017a\u017b\3\2\2\2\u017b\u017e\3\2\2\2\u017c\u017e"+
		"\7\62\2\2\u017d\u0177\3\2\2\2\u017d\u017c\3\2\2\2\u017e\35\3\2\2\2\u017f"+
		"\u0184\7\67\2\2\u0180\u0181\7\64\2\2\u0181\u0183\7\67\2\2\u0182\u0180"+
		"\3\2\2\2\u0183\u0186\3\2\2\2\u0184\u0182\3\2\2\2\u0184\u0185\3\2\2\2\u0185"+
		"\u0189\3\2\2\2\u0186\u0184\3\2\2\2\u0187\u0188\7\64\2\2\u0188\u018a\7"+
		"\62\2\2\u0189\u0187\3\2\2\2\u0189\u018a\3\2\2\2\u018a\37\3\2\2\2\u018b"+
		"\u018c\7\67\2\2\u018c!\3\2\2\2\u018d\u0192\5\34\17\2\u018e\u018f\7\63"+
		"\2\2\u018f\u0191\5\34\17\2\u0190\u018e\3\2\2\2\u0191\u0194\3\2\2\2\u0192"+
		"\u0190\3\2\2\2\u0192\u0193\3\2\2\2\u0193#\3\2\2\2\u0194\u0192\3\2\2\2"+
		"\u0195\u0196\7\7\2\2\u0196\u0197\5\32\16\2\u0197%\3\2\2\2\u0198\u0199"+
		"\t\3\2\2\u0199\'\3\2\2\2\u019a\u019b\7\b\2\2\u019b\u019c\5*\26\2\u019c"+
		")\3\2\2\2\u019d\u01a2\5\34\17\2\u019e\u019f\7\63\2\2\u019f\u01a1\5\34"+
		"\17\2\u01a0\u019e\3\2\2\2\u01a1\u01a4\3\2\2\2\u01a2\u01a0\3\2\2\2\u01a2"+
		"\u01a3\3\2\2\2\u01a3+\3\2\2\2\u01a4\u01a2\3\2\2\2\u01a5\u01a6\5\36\20"+
		"\2\u01a6-\3\2\2\2\u01a7\u01ac\5,\27\2\u01a8\u01a9\7\63\2\2\u01a9\u01ab"+
		"\5,\27\2\u01aa\u01a8\3\2\2\2\u01ab\u01ae\3\2\2\2\u01ac\u01aa\3\2\2\2\u01ac"+
		"\u01ad\3\2\2\2\u01ad/\3\2\2\2\u01ae\u01ac\3\2\2\2\u01af\u01b0\7\n\2\2"+
		"\u01b0\61\3\2\2\2\u01b1\u01b2\7\13\2\2\u01b2\63\3\2\2\2\u01b3\u01b4\7"+
		"\61\2\2\u01b4\65\3\2\2\2\u01b5\u01b6\7\16\2\2\u01b6\u01b7\7\60\2\2\u01b7"+
		"\u01b8\5\64\33\2\u01b8\67\3\2\2\2\u01b9\u01ba\7\17\2\2\u01ba9\3\2\2\2"+
		"\u01bb\u01bc\7\20\2\2\u01bc;\3\2\2\2\u01bd\u01be\7\67\2\2\u01be=\3\2\2"+
		"\2\u01bf\u01c4\5<\37\2\u01c0\u01c1\7\63\2\2\u01c1\u01c3\5<\37\2\u01c2"+
		"\u01c0\3\2\2\2\u01c3\u01c6\3\2\2\2\u01c4\u01c2\3\2\2\2\u01c4\u01c5\3\2"+
		"\2\2\u01c5?\3\2\2\2\u01c6\u01c4\3\2\2\2\u01c7\u01c8\7\21\2\2\u01c8\u01c9"+
		"\5> \2\u01c9A\3\2\2\2\u01ca\u01cb\7\67\2\2\u01cbC\3\2\2\2\u01cc\u01d1"+
		"\5B\"\2\u01cd\u01ce\7\63\2\2\u01ce\u01d0\5B\"\2\u01cf\u01cd\3\2\2\2\u01d0"+
		"\u01d3\3\2\2\2\u01d1\u01cf\3\2\2\2\u01d1\u01d2\3\2\2\2\u01d2E\3\2\2\2"+
		"\u01d3\u01d1\3\2\2\2\u01d4\u01d5\7\22\2\2\u01d5\u01d6\5D#\2\u01d6\u01d7"+
		"\7\5\2\2\u01d7\u01d8\5P)\2\u01d8\u01d9\7\r\2\2\u01d9G\3\2\2\2\u01da\u01db"+
		"\7\23\2\2\u01db\u01dc\5J&\2\u01dcI\3\2\2\2\u01dd\u01de\t\4\2\2\u01deK"+
		"\3\2\2\2\u01df\u01e0\7-\2\2\u01e0\u01e1\7\60\2\2\u01e1\u01e2\5N(\2\u01e2"+
		"M\3\2\2\2\u01e3\u01e4\7\67\2\2\u01e4O\3\2\2\2\u01e5\u01e6\7\61\2\2\u01e6"+
		"Q\3\2\2\2\u01e7\u01e8\7\67\2\2\u01e8S\3\2\2\2\u01e9\u01ee\5R*\2\u01ea"+
		"\u01eb\7\63\2\2\u01eb\u01ed\5R*\2\u01ec\u01ea\3\2\2\2\u01ed\u01f0\3\2"+
		"\2\2\u01ee\u01ec\3\2\2\2\u01ee\u01ef\3\2\2\2\u01efU\3\2\2\2\u01f0\u01ee"+
		"\3\2\2\2\u01f1\u01f2\7\f\2\2\u01f2\u01f3\5T+\2\u01f3\u01f4\7\5\2\2\u01f4"+
		"\u01f5\5P)\2\u01f5\u01f6\7\r\2\2\u01f6W\3\2\2\2\u01f7\u01f8\7\25\2\2\u01f8"+
		"\u01f9\5Z.\2\u01f9Y\3\2\2\2\u01fa\u01fb\t\5\2\2\u01fb[\3\2\2\2\u01fc\u01ff"+
		"\7\61\2\2\u01fd\u01fe\7\63\2\2\u01fe\u0200\7\61\2\2\u01ff\u01fd\3\2\2"+
		"\2\u01ff\u0200\3\2\2\2\u0200]\3\2\2\2\u0201\u0202\7\65\2\2\u0202\u0203"+
		"\5\\/\2\u0203\u0204\7\66\2\2\u0204_\3\2\2\2\u0205\u020a\5^\60\2\u0206"+
		"\u0207\7\63\2\2\u0207\u0209\5^\60\2\u0208\u0206\3\2\2\2\u0209\u020c\3"+
		"\2\2\2\u020a\u0208\3\2\2\2\u020a\u020b\3\2\2\2\u020ba\3\2\2\2\u020c\u020a"+
		"\3\2\2\2\u020d\u020e\7\26\2\2\u020e\u020f\5`\61\2\u020fc\3\2\2\2\u0210"+
		"\u0211\7$\2\2\u0211\u0212\5f\64\2\u0212e\3\2\2\2\u0213\u0214\7\61\2\2"+
		"\u0214g\3\2\2\2\u0215\u0216\7\35\2\2\u0216\u0217\5j\66\2\u0217\u0218\7"+
		"\36\2\2\u0218\u0219\5l\67\2\u0219i\3\2\2\2\u021a\u021b\7\61\2\2\u021b"+
		"k\3\2\2\2\u021c\u021d\7\61\2\2\u021dm\3\2\2\2\u021e\u021f\7 \2\2\u021f"+
		"o\3\2\2\2\u0220\u0221\7\37\2\2\u0221\u0222\5t;\2\u0222q\3\2\2\2\u0223"+
		"\u0224\7\32\2\2\u0224s\3\2\2\2\u0225\u0226\7!\2\2\u0226u\3\2\2\2\u0227"+
		"\u0228\7\"\2\2\u0228\u0229\5x=\2\u0229w\3\2\2\2\u022a\u022b\7\61\2\2\u022b"+
		"y\3\2\2\2\u022c\u022d\7#\2\2\u022d\u0231\5|?\2\u022e\u0230\5~@\2\u022f"+
		"\u022e\3\2\2\2\u0230\u0233\3\2\2\2\u0231\u022f\3\2\2\2\u0231\u0232\3\2"+
		"\2\2\u0232{\3\2\2\2\u0233\u0231\3\2\2\2\u0234\u023b\7\67\2\2\u0235\u0237"+
		"\t\6\2\2\u0236\u0235\3\2\2\2\u0237\u0238\3\2\2\2\u0238\u0236\3\2\2\2\u0238"+
		"\u0239\3\2\2\2\u0239\u023b\3\2\2\2\u023a\u0234\3\2\2\2\u023a\u0236\3\2"+
		"\2\2\u023b}\3\2\2\2\u023c\u023d\7\65\2\2\u023d\u0242\5\u0080A\2\u023e"+
		"\u023f\7\63\2\2\u023f\u0241\5\u0080A\2\u0240\u023e\3\2\2\2\u0241\u0244"+
		"\3\2\2\2\u0242\u0240\3\2\2\2\u0242\u0243\3\2\2\2\u0243\u0245\3\2\2\2\u0244"+
		"\u0242\3\2\2\2\u0245\u0246\7\66\2\2\u0246\177\3\2\2\2\u0247\u0248\5\u0082"+
		"B\2\u0248\u0249\7\60\2\2\u0249\u024a\5\u0084C\2\u024a\u0081\3\2\2\2\u024b"+
		"\u0252\7\67\2\2\u024c\u024e\t\6\2\2\u024d\u024c\3\2\2\2\u024e\u024f\3"+
		"\2\2\2\u024f\u024d\3\2\2\2\u024f\u0250\3\2\2\2\u0250\u0252\3\2\2\2\u0251"+
		"\u024b\3\2\2\2\u0251\u024d\3\2\2\2\u0252\u0083\3\2\2\2\u0253\u025b\7\67"+
		"\2\2\u0254\u0256\t\6\2\2\u0255\u0254\3\2\2\2\u0256\u0257\3\2\2\2\u0257"+
		"\u0255\3\2\2\2\u0257\u0258\3\2\2\2\u0258\u025b\3\2\2\2\u0259\u025b\7\61"+
		"\2\2\u025a\u0253\3\2\2\2\u025a\u0255\3\2\2\2\u025a\u0259\3\2\2\2\u025b"+
		"\u0085\3\2\2\2\u025c\u025d\7\27\2\2\u025d\u025e\7\65\2\2\u025e\u0263\5"+
		"\u00acW\2\u025f\u0260\7\63\2\2\u0260\u0262\5\u00acW\2\u0261\u025f\3\2"+
		"\2\2\u0262\u0265\3\2\2\2\u0263\u0261\3\2\2\2\u0263\u0264\3\2\2\2\u0264"+
		"\u0266\3\2\2\2\u0265\u0263\3\2\2\2\u0266\u0267\7\66\2\2\u0267\u0087\3"+
		"\2\2\2\u0268\u0269\7\30\2\2\u0269\u026a\7\65\2\2\u026a\u026f\5\u008aF"+
		"\2\u026b\u026c\7\63\2\2\u026c\u026e\5\u008aF\2\u026d\u026b\3\2\2\2\u026e"+
		"\u0271\3\2\2\2\u026f\u026d\3\2\2\2\u026f\u0270\3\2\2\2\u0270\u0272\3\2"+
		"\2\2\u0271\u026f\3\2\2\2\u0272\u0273\7\66\2\2\u0273\u0089\3\2\2\2\u0274"+
		"\u0275\t\5\2\2\u0275\u008b\3\2\2\2\u0276\u0277\7\31\2\2\u0277\u0278\7"+
		"\60\2\2\u0278\u0279\5\u008eH\2\u0279\u008d\3\2\2\2\u027a\u027b\79\2\2"+
		"\u027b\u008f\3\2\2\2\u027c\u027d\7%\2\2\u027d\u027e\7\60\2\2\u027e\u027f"+
		"\5\u0092J\2\u027f\u0091\3\2\2\2\u0280\u0281\t\3\2\2\u0281\u0093\3\2\2"+
		"\2\u0282\u0283\7\33\2\2\u0283\u0284\5\u0096L\2\u0284\u0095\3\2\2\2\u0285"+
		"\u0286\7\67\2\2\u0286\u0097\3\2\2\2\u0287\u0288\7&\2\2\u0288\u0289\7\60"+
		"\2\2\u0289\u028a\5\u009aN\2\u028a\u0099\3\2\2\2\u028b\u028c\t\5\2\2\u028c"+
		"\u009b\3\2\2\2\u028d\u028e\7\'\2\2\u028e\u028f\7\60\2\2\u028f\u0290\5"+
		"\u009eP\2\u0290\u009d\3\2\2\2\u0291\u0292\t\5\2\2\u0292\u009f\3\2\2\2"+
		"\u0293\u0294\7(\2\2\u0294\u0295\7\60\2\2\u0295\u0296\5\u00a2R\2\u0296"+
		"\u00a1\3\2\2\2\u0297\u0298\t\7\2\2\u0298\u00a3\3\2\2\2\u0299\u029a\7*"+
		"\2\2\u029a\u029b\7\60\2\2\u029b\u029c\5\u00a6T\2\u029c\u00a5\3\2\2\2\u029d"+
		"\u029f\t\b\2\2\u029e\u029d\3\2\2\2\u029f\u02a0\3\2\2\2\u02a0\u029e\3\2"+
		"\2\2\u02a0\u02a1\3\2\2\2\u02a1\u02a8\3\2\2\2\u02a2\u02a4\t\6\2\2\u02a3"+
		"\u02a2\3\2\2\2\u02a4\u02a5\3\2\2\2\u02a5\u02a3\3\2\2\2\u02a5\u02a6\3\2"+
		"\2\2\u02a6\u02a8\3\2\2\2\u02a7\u029e\3\2\2\2\u02a7\u02a3\3\2\2\2\u02a8"+
		"\u00a7\3\2\2\2\u02a9\u02aa\7)\2\2\u02aa\u02ab\7\60\2\2\u02ab\u02ac\5\u00aa"+
		"V\2\u02ac\u00a9\3\2\2\2\u02ad\u02ae\t\7\2\2\u02ae\u00ab\3\2\2\2\u02af"+
		"\u02b4\5\u00aeX\2\u02b0\u02b1\7\60\2\2\u02b1\u02b5\5\u00b0Y\2\u02b2\u02b3"+
		"\7\t\2\2\u02b3\u02b5\5\u00b0Y\2\u02b4\u02b0\3\2\2\2\u02b4\u02b2\3\2\2"+
		"\2\u02b4\u02b5\3\2\2\2\u02b5\u00ad\3\2\2\2\u02b6\u02b8\t\b\2\2\u02b7\u02b6"+
		"\3\2\2\2\u02b8\u02b9\3\2\2\2\u02b9\u02b7\3\2\2\2\u02b9\u02ba\3\2\2\2\u02ba"+
		"\u02c1\3\2\2\2\u02bb\u02bd\t\6\2\2\u02bc\u02bb\3\2\2\2\u02bd\u02be\3\2"+
		"\2\2\u02be\u02bc\3\2\2\2\u02be\u02bf\3\2\2\2\u02bf\u02c1\3\2\2\2\u02c0"+
		"\u02b7\3\2\2\2\u02c0\u02bc\3\2\2\2\u02c1\u00af\3\2\2\2\u02c2\u02ca\7\67"+
		"\2\2\u02c3\u02c5\t\6\2\2\u02c4\u02c3\3\2\2\2\u02c5\u02c6\3\2\2\2\u02c6"+
		"\u02c4\3\2\2\2\u02c6\u02c7\3\2\2\2\u02c7\u02ca\3\2\2\2\u02c8\u02ca\7\61"+
		"\2\2\u02c9\u02c2\3\2\2\2\u02c9\u02c4\3\2\2\2\u02c9\u02c8\3\2\2\2\u02ca"+
		"\u00b1\3\2\2\2\u02cb\u02cc\7/\2\2\u02cc\u02cd\7\60\2\2\u02cd\u02ce\5\u00b4"+
		"[\2\u02ce\u00b3\3\2\2\2\u02cf\u02d0\7\61\2\2\u02d0\u00b5\3\2\2\2\u02d1"+
		"\u02d2\7.\2\2\u02d2\u02d3\7\60\2\2\u02d3\u02d4\5\u00b8]\2\u02d4\u00b7"+
		"\3\2\2\2\u02d5\u02dd\7\67\2\2\u02d6\u02d8\t\6\2\2\u02d7\u02d6\3\2\2\2"+
		"\u02d8\u02d9\3\2\2\2\u02d9\u02d7\3\2\2\2\u02d9\u02da\3\2\2\2\u02da\u02dd"+
		"\3\2\2\2\u02db\u02dd\7\61\2\2\u02dc\u02d5\3\2\2\2\u02dc\u02d7\3\2\2\2"+
		"\u02dc\u02db\3\2\2\2\u02dd\u02de\3\2\2\2\u02de\u02dc\3\2\2\2\u02de\u02df"+
		"\3\2\2\2\u02df\u00b9\3\2\2\2\u02e0\u02e1\7+\2\2\u02e1\u02e2\7\60\2\2\u02e2"+
		"\u02e3\5\u00bc_\2\u02e3\u00bb\3\2\2\2\u02e4\u02e5\7\67\2\2\u02e5\u00bd"+
		"\3\2\2\2\u02e6\u02e7\7,\2\2\u02e7\u02e8\7\60\2\2\u02e8\u02e9\5\u00c0a"+
		"\2\u02e9\u00bf\3\2\2\2\u02ea\u02eb\7\61\2\2\u02eb\u00c1\3\2\2\2U\u00c4"+
		"\u00d8\u00e0\u00e3\u00e7\u00ea\u00ed\u00f0\u00f3\u00f6\u00f9\u00fc\u00ff"+
		"\u0102\u0105\u0108\u010b\u010e\u0111\u0114\u0117\u011a\u011d\u0120\u0123"+
		"\u0126\u0129\u012c\u012f\u0132\u0135\u0138\u013d\u0140\u0143\u0146\u0149"+
		"\u014c\u014f\u0152\u0155\u0158\u015b\u015e\u0161\u0164\u016b\u0170\u0175"+
		"\u017a\u017d\u0184\u0189\u0192\u01a2\u01ac\u01c4\u01d1\u01ee\u01ff\u020a"+
		"\u0231\u0238\u023a\u0242\u024f\u0251\u0257\u025a\u0263\u026f\u02a0\u02a5"+
		"\u02a7\u02b4\u02b9\u02be\u02c0\u02c6\u02c9\u02d9\u02dc\u02de";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}