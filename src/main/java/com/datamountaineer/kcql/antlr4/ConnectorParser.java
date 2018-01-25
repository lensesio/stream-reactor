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
		WITHCOMPRESSION=41, WITHDELAY=42, WITHPARTITIONER=43, WITHSUBSCRIPTION=44, 
		TIMESTAMPUNIT=45, WITHPIPELINE=46, TTL=47, EQUAL=48, INT=49, ASTERISK=50, 
		COMMA=51, DOT=52, LEFT_PARAN=53, RIGHT_PARAN=54, FIELD=55, TOPICNAME=56, 
		KEYDELIMVALUE=57, NEWLINE=58, WS=59, ID=60;
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
		RULE_with_delay_value = 95, RULE_with_partitioner_clause = 96, RULE_with_partitioner_value = 97, 
		RULE_with_subscription_clause = 98, RULE_with_subscription_value = 99;
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
		"with_delay_value", "with_partitioner_clause", "with_partitioner_value", 
		"with_subscription_clause", "with_subscription_value"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		"'='", null, "'*'", "','", "'.'", "'('", "')'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "INITIALIZE", 
		"PARTITIONBY", "DISTRIBUTEBY", "TIMESTAMP", "SYS_TIME", "WITHGROUP", "WITHOFFSET", 
		"WITHTAG", "WITHKEY", "KEYDELIM", "WITHSTRUCTURE", "WITHTYPE", "PK", "SAMPLE", 
		"EVERY", "WITHFORMAT", "WITHUNWRAP", "FORMAT", "PROJECTTO", "STOREAS", 
		"LIMIT", "INCREMENTALMODE", "WITHDOCTYPE", "WITHINDEXSUFFIX", "WITHCONVERTER", 
		"WITHJMSSELECTOR", "WITHTARGET", "WITHCOMPRESSION", "WITHDELAY", "WITHPARTITIONER", 
		"WITHSUBSCRIPTION", "TIMESTAMPUNIT", "WITHPIPELINE", "TTL", "EQUAL", "INT", 
		"ASTERISK", "COMMA", "DOT", "LEFT_PARAN", "RIGHT_PARAN", "FIELD", "TOPICNAME", 
		"KEYDELIMVALUE", "NEWLINE", "WS", "ID"
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
			setState(202);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INSERT:
			case UPSERT:
				enterOuterAlt(_localctx, 1);
				{
				setState(200);
				insert_from_clause();
				}
				break;
			case SELECT:
				enterOuterAlt(_localctx, 2);
				{
				setState(201);
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
			setState(204);
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
			setState(206);
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
			setState(208);
			match(INSERT);
			setState(209);
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
			setState(211);
			match(UPSERT);
			setState(212);
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
			setState(214);
			match(UPSERT);
			setState(215);
			pk();
			setState(216);
			match(FIELD);
			setState(217);
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
			setState(222);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(219);
				insert_into();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(220);
				upsert_into();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(221);
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
			setState(224);
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
		public With_partitioner_clauseContext with_partitioner_clause() {
			return getRuleContext(With_partitioner_clauseContext.class,0);
		}
		public With_subscription_clauseContext with_subscription_clause() {
			return getRuleContext(With_subscription_clauseContext.class,0);
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
			setState(226);
			write_mode();
			setState(227);
			table_name();
			setState(228);
			select_clause_basic();
			setState(230);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AUTOCREATE) {
				{
				setState(229);
				autocreate();
				}
			}

			setState(233);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHSTRUCTURE) {
				{
				setState(232);
				with_structure();
				}
			}

			setState(237);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PK) {
				{
				setState(235);
				match(PK);
				setState(236);
				primary_key_list();
				}
			}

			setState(240);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHTARGET) {
				{
				setState(239);
				with_target();
				}
			}

			setState(243);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AUTOEVOLVE) {
				{
				setState(242);
				autoevolve();
				}
			}

			setState(246);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==BATCH) {
				{
				setState(245);
				batching();
				}
			}

			setState(249);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CAPITALIZE) {
				{
				setState(248);
				capitalize();
				}
			}

			setState(252);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INITIALIZE) {
				{
				setState(251);
				initialize();
				}
			}

			setState(255);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PROJECTTO) {
				{
				setState(254);
				project_to();
				}
			}

			setState(258);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PARTITIONBY) {
				{
				setState(257);
				partitionby();
				}
			}

			setState(261);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DISTRIBUTEBY) {
				{
				setState(260);
				distributeby();
				}
			}

			setState(264);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CLUSTERBY) {
				{
				setState(263);
				clusterby();
				}
			}

			setState(267);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TIMESTAMP) {
				{
				setState(266);
				timestamp_clause();
				}
			}

			setState(270);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TIMESTAMPUNIT) {
				{
				setState(269);
				timestamp_unit_clause();
				}
			}

			setState(273);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHFORMAT) {
				{
				setState(272);
				with_format_clause();
				}
			}

			setState(276);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHUNWRAP) {
				{
				setState(275);
				with_unwrap_clause();
				}
			}

			setState(279);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STOREAS) {
				{
				setState(278);
				storeas_clause();
				}
			}

			setState(282);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHTAG) {
				{
				setState(281);
				with_tags();
				}
			}

			setState(285);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INCREMENTALMODE) {
				{
				setState(284);
				with_inc_mode();
				}
			}

			setState(288);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHTYPE) {
				{
				setState(287);
				with_type();
				}
			}

			setState(291);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHDOCTYPE) {
				{
				setState(290);
				with_doc_type();
				}
			}

			setState(294);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHINDEXSUFFIX) {
				{
				setState(293);
				with_index_suffix();
				}
			}

			setState(297);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TTL) {
				{
				setState(296);
				ttl_clause();
				}
			}

			setState(300);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHCONVERTER) {
				{
				setState(299);
				with_converter();
				}
			}

			setState(303);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHJMSSELECTOR) {
				{
				setState(302);
				with_jms_selector();
				}
			}

			setState(306);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHKEY) {
				{
				setState(305);
				with_key();
				}
			}

			setState(309);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==KEYDELIM) {
				{
				setState(308);
				key_delimiter();
				}
			}

			setState(312);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHCOMPRESSION) {
				{
				setState(311);
				with_compression_clause();
				}
			}

			setState(315);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHDELAY) {
				{
				setState(314);
				with_delay_clause();
				}
			}

			setState(318);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHPIPELINE) {
				{
				setState(317);
				with_pipeline_clause();
				}
			}

			setState(321);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHPARTITIONER) {
				{
				setState(320);
				with_partitioner_clause();
				}
			}

			setState(324);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHSUBSCRIPTION) {
				{
				setState(323);
				with_subscription_clause();
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
			setState(326);
			select_clause_basic();
			setState(329);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PK) {
				{
				setState(327);
				match(PK);
				setState(328);
				primary_key_list();
				}
			}

			setState(332);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHSTRUCTURE) {
				{
				setState(331);
				with_structure();
				}
			}

			setState(335);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHFORMAT) {
				{
				setState(334);
				with_format_clause();
				}
			}

			setState(338);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHUNWRAP) {
				{
				setState(337);
				with_unwrap_clause();
				}
			}

			setState(341);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHGROUP) {
				{
				setState(340);
				with_consumer_group();
				}
			}

			setState(344);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHOFFSET) {
				{
				setState(343);
				with_offset_list();
				}
			}

			setState(347);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SAMPLE) {
				{
				setState(346);
				sample_clause();
				}
			}

			setState(350);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(349);
				limit_clause();
				}
			}

			setState(353);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STOREAS) {
				{
				setState(352);
				storeas_clause();
				}
			}

			setState(356);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHTAG) {
				{
				setState(355);
				with_tags();
				}
			}

			setState(359);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==INCREMENTALMODE) {
				{
				setState(358);
				with_inc_mode();
				}
			}

			setState(362);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHDOCTYPE) {
				{
				setState(361);
				with_doc_type();
				}
			}

			setState(365);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHINDEXSUFFIX) {
				{
				setState(364);
				with_index_suffix();
				}
			}

			setState(368);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WITHCONVERTER) {
				{
				setState(367);
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
			setState(370);
			match(SELECT);
			setState(371);
			column_list();
			setState(372);
			match(FROM);
			setState(373);
			topic_name();
			setState(375);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==IGNORE) {
				{
				setState(374);
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
			setState(378); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(377);
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
				setState(380); 
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
			setState(383); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(382);
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
				setState(385); 
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
			setState(393);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(387);
				column();
				setState(390);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(388);
					match(AS);
					setState(389);
					column_name_alias();
					}
				}

				}
				break;
			case ASTERISK:
				enterOuterAlt(_localctx, 2);
				{
				setState(392);
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
			setState(395);
			match(FIELD);
			setState(400);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,53,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(396);
					match(DOT);
					setState(397);
					match(FIELD);
					}
					} 
				}
				setState(402);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,53,_ctx);
			}
			setState(405);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DOT) {
				{
				setState(403);
				match(DOT);
				setState(404);
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
			setState(407);
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
			setState(409);
			column_name();
			setState(414);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(410);
				match(COMMA);
				setState(411);
				column_name();
				}
				}
				setState(416);
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
			setState(417);
			match(FROM);
			setState(418);
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
			setState(420);
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
			setState(422);
			match(IGNORE);
			setState(423);
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
			setState(425);
			column_name();
			setState(430);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(426);
				match(COMMA);
				setState(427);
				column_name();
				}
				}
				setState(432);
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
			setState(433);
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
			setState(435);
			pk_name();
			setState(440);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(436);
				match(COMMA);
				setState(437);
				pk_name();
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
			setState(443);
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
			setState(445);
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
			setState(447);
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
			setState(449);
			match(BATCH);
			setState(450);
			match(EQUAL);
			setState(451);
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
			setState(453);
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
			setState(455);
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
			setState(457);
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
			setState(459);
			partition_name();
			setState(464);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(460);
				match(COMMA);
				setState(461);
				partition_name();
				}
				}
				setState(466);
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
			setState(467);
			match(PARTITIONBY);
			setState(468);
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
			setState(470);
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
			setState(472);
			distribute_name();
			setState(477);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(473);
				match(COMMA);
				setState(474);
				distribute_name();
				}
				}
				setState(479);
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
			setState(480);
			match(DISTRIBUTEBY);
			setState(481);
			distribute_list();
			setState(482);
			match(INTO);
			setState(483);
			buckets_number();
			setState(484);
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
			setState(486);
			match(TIMESTAMP);
			setState(487);
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
			setState(489);
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
			setState(491);
			match(TIMESTAMPUNIT);
			setState(492);
			match(EQUAL);
			setState(493);
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
			setState(495);
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
			setState(497);
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
			setState(499);
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
			setState(501);
			clusterby_name();
			setState(506);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(502);
				match(COMMA);
				setState(503);
				clusterby_name();
				}
				}
				setState(508);
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
			setState(509);
			match(CLUSTERBY);
			setState(510);
			clusterby_list();
			setState(511);
			match(INTO);
			setState(512);
			buckets_number();
			setState(513);
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
			setState(515);
			match(WITHGROUP);
			setState(516);
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
			setState(518);
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
			setState(520);
			match(INT);
			setState(523);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(521);
				match(COMMA);
				setState(522);
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
			setState(525);
			match(LEFT_PARAN);
			setState(526);
			offset_partition_inner();
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
			setState(529);
			offset_partition();
			setState(534);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(530);
				match(COMMA);
				setState(531);
				offset_partition();
				}
				}
				setState(536);
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
			setState(537);
			match(WITHOFFSET);
			setState(538);
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
			setState(540);
			match(LIMIT);
			setState(541);
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
			setState(543);
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
			setState(545);
			match(SAMPLE);
			setState(546);
			sample_value();
			setState(547);
			match(EVERY);
			setState(548);
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
			setState(550);
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
			setState(554);
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
			setState(556);
			match(WITHFORMAT);
			setState(557);
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
			setState(559);
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
			setState(561);
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
			setState(563);
			match(PROJECTTO);
			setState(564);
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
			setState(566);
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
			setState(568);
			match(STOREAS);
			setState(569);
			storeas_type();
			setState(573);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==LEFT_PARAN) {
				{
				{
				setState(570);
				storeas_parameters();
				}
				}
				setState(575);
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
			setState(582);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(576);
				match(FIELD);
				}
				break;
			case DOT:
			case TOPICNAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(578); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(577);
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
					setState(580); 
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
			setState(584);
			match(LEFT_PARAN);
			setState(585);
			storeas_parameters_tuple();
			setState(590);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(586);
				match(COMMA);
				setState(587);
				storeas_parameters_tuple();
				}
				}
				setState(592);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(593);
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
			setState(595);
			storeas_parameter();
			setState(596);
			match(EQUAL);
			setState(597);
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
			setState(605);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(599);
				match(FIELD);
				}
				break;
			case DOT:
			case TOPICNAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(601); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(600);
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
					setState(603); 
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
			setState(614);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(607);
				match(FIELD);
				}
				break;
			case DOT:
			case TOPICNAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(609); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(608);
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
					setState(611); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==TOPICNAME );
				}
				break;
			case INT:
				enterOuterAlt(_localctx, 3);
				{
				setState(613);
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
			setState(616);
			match(WITHTAG);
			{
			setState(617);
			match(LEFT_PARAN);
			setState(618);
			tag_definition();
			setState(623);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(619);
				match(COMMA);
				setState(620);
				tag_definition();
				}
				}
				setState(625);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(626);
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
			setState(628);
			match(WITHKEY);
			{
			setState(629);
			match(LEFT_PARAN);
			setState(630);
			with_key_value();
			setState(635);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(631);
				match(COMMA);
				setState(632);
				with_key_value();
				}
				}
				setState(637);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(638);
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
			setState(640);
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
			setState(642);
			match(KEYDELIM);
			setState(643);
			match(EQUAL);
			setState(644);
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
			setState(646);
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
			setState(648);
			match(INCREMENTALMODE);
			setState(649);
			match(EQUAL);
			setState(650);
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
			setState(652);
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
			setState(654);
			match(WITHTYPE);
			setState(655);
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
			setState(657);
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
			setState(659);
			match(WITHDOCTYPE);
			setState(660);
			match(EQUAL);
			setState(661);
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
			setState(663);
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
			setState(665);
			match(WITHINDEXSUFFIX);
			setState(666);
			match(EQUAL);
			setState(667);
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
			setState(669);
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
			setState(671);
			match(WITHCONVERTER);
			setState(672);
			match(EQUAL);
			setState(673);
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
			setState(675);
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
			setState(677);
			match(WITHTARGET);
			setState(678);
			match(EQUAL);
			setState(679);
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
			setState(691);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,75,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(682); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(681);
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
					setState(684); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==FIELD );
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(687); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(686);
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
					setState(689); 
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
			setState(693);
			match(WITHJMSSELECTOR);
			setState(694);
			match(EQUAL);
			setState(695);
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
			setState(697);
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
			setState(699);
			tag_key();
			setState(704);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case EQUAL:
				{
				{
				setState(700);
				match(EQUAL);
				setState(701);
				tag_value();
				}
				}
				break;
			case AS:
				{
				{
				setState(702);
				match(AS);
				setState(703);
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
			setState(716);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,79,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(707); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(706);
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
					setState(709); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==FIELD );
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(712); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(711);
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
					setState(714); 
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
			setState(725);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case FIELD:
				enterOuterAlt(_localctx, 1);
				{
				setState(718);
				match(FIELD);
				}
				break;
			case DOT:
			case TOPICNAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(720); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(719);
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
					setState(722); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==DOT || _la==TOPICNAME );
				}
				break;
			case INT:
				enterOuterAlt(_localctx, 3);
				{
				setState(724);
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
			setState(727);
			match(TTL);
			setState(728);
			match(EQUAL);
			setState(729);
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
			setState(731);
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
			setState(733);
			match(WITHPIPELINE);
			setState(734);
			match(EQUAL);
			setState(735);
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
			setState(744); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				setState(744);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case FIELD:
					{
					setState(737);
					match(FIELD);
					}
					break;
				case DOT:
				case TOPICNAME:
					{
					setState(739); 
					_errHandler.sync(this);
					_alt = 1;
					do {
						switch (_alt) {
						case 1:
							{
							{
							setState(738);
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
						setState(741); 
						_errHandler.sync(this);
						_alt = getInterpreter().adaptivePredict(_input,82,_ctx);
					} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
					}
					break;
				case INT:
					{
					setState(743);
					match(INT);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(746); 
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
			setState(748);
			match(WITHCOMPRESSION);
			setState(749);
			match(EQUAL);
			setState(750);
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
			setState(752);
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
			setState(754);
			match(WITHDELAY);
			setState(755);
			match(EQUAL);
			setState(756);
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
			setState(758);
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

	public static class With_partitioner_clauseContext extends ParserRuleContext {
		public TerminalNode WITHPARTITIONER() { return getToken(ConnectorParser.WITHPARTITIONER, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public With_partitioner_valueContext with_partitioner_value() {
			return getRuleContext(With_partitioner_valueContext.class,0);
		}
		public With_partitioner_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_partitioner_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_partitioner_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_partitioner_clause(this);
		}
	}

	public final With_partitioner_clauseContext with_partitioner_clause() throws RecognitionException {
		With_partitioner_clauseContext _localctx = new With_partitioner_clauseContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_with_partitioner_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(760);
			match(WITHPARTITIONER);
			setState(761);
			match(EQUAL);
			setState(762);
			with_partitioner_value();
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

	public static class With_partitioner_valueContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public With_partitioner_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_partitioner_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_partitioner_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_partitioner_value(this);
		}
	}

	public final With_partitioner_valueContext with_partitioner_value() throws RecognitionException {
		With_partitioner_valueContext _localctx = new With_partitioner_valueContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_with_partitioner_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(764);
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

	public static class With_subscription_clauseContext extends ParserRuleContext {
		public TerminalNode WITHSUBSCRIPTION() { return getToken(ConnectorParser.WITHSUBSCRIPTION, 0); }
		public TerminalNode EQUAL() { return getToken(ConnectorParser.EQUAL, 0); }
		public With_subscription_valueContext with_subscription_value() {
			return getRuleContext(With_subscription_valueContext.class,0);
		}
		public With_subscription_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_subscription_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_subscription_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_subscription_clause(this);
		}
	}

	public final With_subscription_clauseContext with_subscription_clause() throws RecognitionException {
		With_subscription_clauseContext _localctx = new With_subscription_clauseContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_with_subscription_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(766);
			match(WITHSUBSCRIPTION);
			setState(767);
			match(EQUAL);
			setState(768);
			with_subscription_value();
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

	public static class With_subscription_valueContext extends ParserRuleContext {
		public TerminalNode FIELD() { return getToken(ConnectorParser.FIELD, 0); }
		public With_subscription_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_with_subscription_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).enterWith_subscription_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof ConnectorParserListener ) ((ConnectorParserListener)listener).exitWith_subscription_value(this);
		}
	}

	public final With_subscription_valueContext with_subscription_value() throws RecognitionException {
		With_subscription_valueContext _localctx = new With_subscription_valueContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_with_subscription_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(770);
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

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3>\u0307\4\2\t\2\4"+
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
		"`\t`\4a\ta\4b\tb\4c\tc\4d\td\4e\te\3\2\3\2\5\2\u00cd\n\2\3\3\3\3\3\4\3"+
		"\4\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\5\b\u00e1\n"+
		"\b\3\t\3\t\3\n\3\n\3\n\3\n\5\n\u00e9\n\n\3\n\5\n\u00ec\n\n\3\n\3\n\5\n"+
		"\u00f0\n\n\3\n\5\n\u00f3\n\n\3\n\5\n\u00f6\n\n\3\n\5\n\u00f9\n\n\3\n\5"+
		"\n\u00fc\n\n\3\n\5\n\u00ff\n\n\3\n\5\n\u0102\n\n\3\n\5\n\u0105\n\n\3\n"+
		"\5\n\u0108\n\n\3\n\5\n\u010b\n\n\3\n\5\n\u010e\n\n\3\n\5\n\u0111\n\n\3"+
		"\n\5\n\u0114\n\n\3\n\5\n\u0117\n\n\3\n\5\n\u011a\n\n\3\n\5\n\u011d\n\n"+
		"\3\n\5\n\u0120\n\n\3\n\5\n\u0123\n\n\3\n\5\n\u0126\n\n\3\n\5\n\u0129\n"+
		"\n\3\n\5\n\u012c\n\n\3\n\5\n\u012f\n\n\3\n\5\n\u0132\n\n\3\n\5\n\u0135"+
		"\n\n\3\n\5\n\u0138\n\n\3\n\5\n\u013b\n\n\3\n\5\n\u013e\n\n\3\n\5\n\u0141"+
		"\n\n\3\n\5\n\u0144\n\n\3\n\5\n\u0147\n\n\3\13\3\13\3\13\5\13\u014c\n\13"+
		"\3\13\5\13\u014f\n\13\3\13\5\13\u0152\n\13\3\13\5\13\u0155\n\13\3\13\5"+
		"\13\u0158\n\13\3\13\5\13\u015b\n\13\3\13\5\13\u015e\n\13\3\13\5\13\u0161"+
		"\n\13\3\13\5\13\u0164\n\13\3\13\5\13\u0167\n\13\3\13\5\13\u016a\n\13\3"+
		"\13\5\13\u016d\n\13\3\13\5\13\u0170\n\13\3\13\5\13\u0173\n\13\3\f\3\f"+
		"\3\f\3\f\3\f\5\f\u017a\n\f\3\r\6\r\u017d\n\r\r\r\16\r\u017e\3\16\6\16"+
		"\u0182\n\16\r\16\16\16\u0183\3\17\3\17\3\17\5\17\u0189\n\17\3\17\5\17"+
		"\u018c\n\17\3\20\3\20\3\20\7\20\u0191\n\20\f\20\16\20\u0194\13\20\3\20"+
		"\3\20\5\20\u0198\n\20\3\21\3\21\3\22\3\22\3\22\7\22\u019f\n\22\f\22\16"+
		"\22\u01a2\13\22\3\23\3\23\3\23\3\24\3\24\3\25\3\25\3\25\3\26\3\26\3\26"+
		"\7\26\u01af\n\26\f\26\16\26\u01b2\13\26\3\27\3\27\3\30\3\30\3\30\7\30"+
		"\u01b9\n\30\f\30\16\30\u01bc\13\30\3\31\3\31\3\32\3\32\3\33\3\33\3\34"+
		"\3\34\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3 \7 \u01d1\n \f "+
		"\16 \u01d4\13 \3!\3!\3!\3\"\3\"\3#\3#\3#\7#\u01de\n#\f#\16#\u01e1\13#"+
		"\3$\3$\3$\3$\3$\3$\3%\3%\3%\3&\3&\3\'\3\'\3\'\3\'\3(\3(\3)\3)\3*\3*\3"+
		"+\3+\3+\7+\u01fb\n+\f+\16+\u01fe\13+\3,\3,\3,\3,\3,\3,\3-\3-\3-\3.\3."+
		"\3/\3/\3/\5/\u020e\n/\3\60\3\60\3\60\3\60\3\61\3\61\3\61\7\61\u0217\n"+
		"\61\f\61\16\61\u021a\13\61\3\62\3\62\3\62\3\63\3\63\3\63\3\64\3\64\3\65"+
		"\3\65\3\65\3\65\3\65\3\66\3\66\3\67\3\67\38\38\39\39\39\3:\3:\3;\3;\3"+
		"<\3<\3<\3=\3=\3>\3>\3>\7>\u023e\n>\f>\16>\u0241\13>\3?\3?\6?\u0245\n?"+
		"\r?\16?\u0246\5?\u0249\n?\3@\3@\3@\3@\7@\u024f\n@\f@\16@\u0252\13@\3@"+
		"\3@\3A\3A\3A\3A\3B\3B\6B\u025c\nB\rB\16B\u025d\5B\u0260\nB\3C\3C\6C\u0264"+
		"\nC\rC\16C\u0265\3C\5C\u0269\nC\3D\3D\3D\3D\3D\7D\u0270\nD\fD\16D\u0273"+
		"\13D\3D\3D\3E\3E\3E\3E\3E\7E\u027c\nE\fE\16E\u027f\13E\3E\3E\3F\3F\3G"+
		"\3G\3G\3G\3H\3H\3I\3I\3I\3I\3J\3J\3K\3K\3K\3L\3L\3M\3M\3M\3M\3N\3N\3O"+
		"\3O\3O\3O\3P\3P\3Q\3Q\3Q\3Q\3R\3R\3S\3S\3S\3S\3T\6T\u02ad\nT\rT\16T\u02ae"+
		"\3T\6T\u02b2\nT\rT\16T\u02b3\5T\u02b6\nT\3U\3U\3U\3U\3V\3V\3W\3W\3W\3"+
		"W\3W\5W\u02c3\nW\3X\6X\u02c6\nX\rX\16X\u02c7\3X\6X\u02cb\nX\rX\16X\u02cc"+
		"\5X\u02cf\nX\3Y\3Y\6Y\u02d3\nY\rY\16Y\u02d4\3Y\5Y\u02d8\nY\3Z\3Z\3Z\3"+
		"Z\3[\3[\3\\\3\\\3\\\3\\\3]\3]\6]\u02e6\n]\r]\16]\u02e7\3]\6]\u02eb\n]"+
		"\r]\16]\u02ec\3^\3^\3^\3^\3_\3_\3`\3`\3`\3`\3a\3a\3b\3b\3b\3b\3c\3c\3"+
		"d\3d\3d\3d\3e\3e\3e\2\2f\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&("+
		"*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084"+
		"\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098\u009a\u009c"+
		"\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4"+
		"\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\u00c2\u00c4\u00c6\u00c8\2\t\4\2\66"+
		"\669:\3\29:\4\2\24\2499\4\2\63\639:\4\2\66\66::\4\2::>>\4\2\66\6699\2"+
		"\u02fc\2\u00cc\3\2\2\2\4\u00ce\3\2\2\2\6\u00d0\3\2\2\2\b\u00d2\3\2\2\2"+
		"\n\u00d5\3\2\2\2\f\u00d8\3\2\2\2\16\u00e0\3\2\2\2\20\u00e2\3\2\2\2\22"+
		"\u00e4\3\2\2\2\24\u0148\3\2\2\2\26\u0174\3\2\2\2\30\u017c\3\2\2\2\32\u0181"+
		"\3\2\2\2\34\u018b\3\2\2\2\36\u018d\3\2\2\2 \u0199\3\2\2\2\"\u019b\3\2"+
		"\2\2$\u01a3\3\2\2\2&\u01a6\3\2\2\2(\u01a8\3\2\2\2*\u01ab\3\2\2\2,\u01b3"+
		"\3\2\2\2.\u01b5\3\2\2\2\60\u01bd\3\2\2\2\62\u01bf\3\2\2\2\64\u01c1\3\2"+
		"\2\2\66\u01c3\3\2\2\28\u01c7\3\2\2\2:\u01c9\3\2\2\2<\u01cb\3\2\2\2>\u01cd"+
		"\3\2\2\2@\u01d5\3\2\2\2B\u01d8\3\2\2\2D\u01da\3\2\2\2F\u01e2\3\2\2\2H"+
		"\u01e8\3\2\2\2J\u01eb\3\2\2\2L\u01ed\3\2\2\2N\u01f1\3\2\2\2P\u01f3\3\2"+
		"\2\2R\u01f5\3\2\2\2T\u01f7\3\2\2\2V\u01ff\3\2\2\2X\u0205\3\2\2\2Z\u0208"+
		"\3\2\2\2\\\u020a\3\2\2\2^\u020f\3\2\2\2`\u0213\3\2\2\2b\u021b\3\2\2\2"+
		"d\u021e\3\2\2\2f\u0221\3\2\2\2h\u0223\3\2\2\2j\u0228\3\2\2\2l\u022a\3"+
		"\2\2\2n\u022c\3\2\2\2p\u022e\3\2\2\2r\u0231\3\2\2\2t\u0233\3\2\2\2v\u0235"+
		"\3\2\2\2x\u0238\3\2\2\2z\u023a\3\2\2\2|\u0248\3\2\2\2~\u024a\3\2\2\2\u0080"+
		"\u0255\3\2\2\2\u0082\u025f\3\2\2\2\u0084\u0268\3\2\2\2\u0086\u026a\3\2"+
		"\2\2\u0088\u0276\3\2\2\2\u008a\u0282\3\2\2\2\u008c\u0284\3\2\2\2\u008e"+
		"\u0288\3\2\2\2\u0090\u028a\3\2\2\2\u0092\u028e\3\2\2\2\u0094\u0290\3\2"+
		"\2\2\u0096\u0293\3\2\2\2\u0098\u0295\3\2\2\2\u009a\u0299\3\2\2\2\u009c"+
		"\u029b\3\2\2\2\u009e\u029f\3\2\2\2\u00a0\u02a1\3\2\2\2\u00a2\u02a5\3\2"+
		"\2\2\u00a4\u02a7\3\2\2\2\u00a6\u02b5\3\2\2\2\u00a8\u02b7\3\2\2\2\u00aa"+
		"\u02bb\3\2\2\2\u00ac\u02bd\3\2\2\2\u00ae\u02ce\3\2\2\2\u00b0\u02d7\3\2"+
		"\2\2\u00b2\u02d9\3\2\2\2\u00b4\u02dd\3\2\2\2\u00b6\u02df\3\2\2\2\u00b8"+
		"\u02ea\3\2\2\2\u00ba\u02ee\3\2\2\2\u00bc\u02f2\3\2\2\2\u00be\u02f4\3\2"+
		"\2\2\u00c0\u02f8\3\2\2\2\u00c2\u02fa\3\2\2\2\u00c4\u02fe\3\2\2\2\u00c6"+
		"\u0300\3\2\2\2\u00c8\u0304\3\2\2\2\u00ca\u00cd\5\22\n\2\u00cb\u00cd\5"+
		"\24\13\2\u00cc\u00ca\3\2\2\2\u00cc\u00cb\3\2\2\2\u00cd\3\3\2\2\2\u00ce"+
		"\u00cf\7\5\2\2\u00cf\5\3\2\2\2\u00d0\u00d1\7\34\2\2\u00d1\7\3\2\2\2\u00d2"+
		"\u00d3\7\3\2\2\u00d3\u00d4\5\4\3\2\u00d4\t\3\2\2\2\u00d5\u00d6\7\4\2\2"+
		"\u00d6\u00d7\5\4\3\2\u00d7\13\3\2\2\2\u00d8\u00d9\7\4\2\2\u00d9\u00da"+
		"\5\6\4\2\u00da\u00db\79\2\2\u00db\u00dc\5\4\3\2\u00dc\r\3\2\2\2\u00dd"+
		"\u00e1\5\b\5\2\u00de\u00e1\5\n\6\2\u00df\u00e1\5\f\7\2\u00e0\u00dd\3\2"+
		"\2\2\u00e0\u00de\3\2\2\2\u00e0\u00df\3\2\2\2\u00e1\17\3\2\2\2\u00e2\u00e3"+
		"\79\2\2\u00e3\21\3\2\2\2\u00e4\u00e5\5\16\b\2\u00e5\u00e6\5\32\16\2\u00e6"+
		"\u00e8\5\26\f\2\u00e7\u00e9\5\60\31\2\u00e8\u00e7\3\2\2\2\u00e8\u00e9"+
		"\3\2\2\2\u00e9\u00eb\3\2\2\2\u00ea\u00ec\5r:\2\u00eb\u00ea\3\2\2\2\u00eb"+
		"\u00ec\3\2\2\2\u00ec\u00ef\3\2\2\2\u00ed\u00ee\7\34\2\2\u00ee\u00f0\5"+
		".\30\2\u00ef\u00ed\3\2\2\2\u00ef\u00f0\3\2\2\2\u00f0\u00f2\3\2\2\2\u00f1"+
		"\u00f3\5\u00a4S\2\u00f2\u00f1\3\2\2\2\u00f2\u00f3\3\2\2\2\u00f3\u00f5"+
		"\3\2\2\2\u00f4\u00f6\5\62\32\2\u00f5\u00f4\3\2\2\2\u00f5\u00f6\3\2\2\2"+
		"\u00f6\u00f8\3\2\2\2\u00f7\u00f9\5\66\34\2\u00f8\u00f7\3\2\2\2\u00f8\u00f9"+
		"\3\2\2\2\u00f9\u00fb\3\2\2\2\u00fa\u00fc\58\35\2\u00fb\u00fa\3\2\2\2\u00fb"+
		"\u00fc\3\2\2\2\u00fc\u00fe\3\2\2\2\u00fd\u00ff\5:\36\2\u00fe\u00fd\3\2"+
		"\2\2\u00fe\u00ff\3\2\2\2\u00ff\u0101\3\2\2\2\u0100\u0102\5v<\2\u0101\u0100"+
		"\3\2\2\2\u0101\u0102\3\2\2\2\u0102\u0104\3\2\2\2\u0103\u0105\5@!\2\u0104"+
		"\u0103\3\2\2\2\u0104\u0105\3\2\2\2\u0105\u0107\3\2\2\2\u0106\u0108\5F"+
		"$\2\u0107\u0106\3\2\2\2\u0107\u0108\3\2\2\2\u0108\u010a\3\2\2\2\u0109"+
		"\u010b\5V,\2\u010a\u0109\3\2\2\2\u010a\u010b\3\2\2\2\u010b\u010d\3\2\2"+
		"\2\u010c\u010e\5H%\2\u010d\u010c\3\2\2\2\u010d\u010e\3\2\2\2\u010e\u0110"+
		"\3\2\2\2\u010f\u0111\5L\'\2\u0110\u010f\3\2\2\2\u0110\u0111\3\2\2\2\u0111"+
		"\u0113\3\2\2\2\u0112\u0114\5p9\2\u0113\u0112\3\2\2\2\u0113\u0114\3\2\2"+
		"\2\u0114\u0116\3\2\2\2\u0115\u0117\5n8\2\u0116\u0115\3\2\2\2\u0116\u0117"+
		"\3\2\2\2\u0117\u0119\3\2\2\2\u0118\u011a\5z>\2\u0119\u0118\3\2\2\2\u0119"+
		"\u011a\3\2\2\2\u011a\u011c\3\2\2\2\u011b\u011d\5\u0086D\2\u011c\u011b"+
		"\3\2\2\2\u011c\u011d\3\2\2\2\u011d\u011f\3\2\2\2\u011e\u0120\5\u0090I"+
		"\2\u011f\u011e\3\2\2\2\u011f\u0120\3\2\2\2\u0120\u0122\3\2\2\2\u0121\u0123"+
		"\5\u0094K\2\u0122\u0121\3\2\2\2\u0122\u0123\3\2\2\2\u0123\u0125\3\2\2"+
		"\2\u0124\u0126\5\u0098M\2\u0125\u0124\3\2\2\2\u0125\u0126\3\2\2\2\u0126"+
		"\u0128\3\2\2\2\u0127\u0129\5\u009cO\2\u0128\u0127\3\2\2\2\u0128\u0129"+
		"\3\2\2\2\u0129\u012b\3\2\2\2\u012a\u012c\5\u00b2Z\2\u012b\u012a\3\2\2"+
		"\2\u012b\u012c\3\2\2\2\u012c\u012e\3\2\2\2\u012d\u012f\5\u00a0Q\2\u012e"+
		"\u012d\3\2\2\2\u012e\u012f\3\2\2\2\u012f\u0131\3\2\2\2\u0130\u0132\5\u00a8"+
		"U\2\u0131\u0130\3\2\2\2\u0131\u0132\3\2\2\2\u0132\u0134\3\2\2\2\u0133"+
		"\u0135\5\u0088E\2\u0134\u0133\3\2\2\2\u0134\u0135\3\2\2\2\u0135\u0137"+
		"\3\2\2\2\u0136\u0138\5\u008cG\2\u0137\u0136\3\2\2\2\u0137\u0138\3\2\2"+
		"\2\u0138\u013a\3\2\2\2\u0139\u013b\5\u00ba^\2\u013a\u0139\3\2\2\2\u013a"+
		"\u013b\3\2\2\2\u013b\u013d\3\2\2\2\u013c\u013e\5\u00be`\2\u013d\u013c"+
		"\3\2\2\2\u013d\u013e\3\2\2\2\u013e\u0140\3\2\2\2\u013f\u0141\5\u00b6\\"+
		"\2\u0140\u013f\3\2\2\2\u0140\u0141\3\2\2\2\u0141\u0143\3\2\2\2\u0142\u0144"+
		"\5\u00c2b\2\u0143\u0142\3\2\2\2\u0143\u0144\3\2\2\2\u0144\u0146\3\2\2"+
		"\2\u0145\u0147\5\u00c6d\2\u0146\u0145\3\2\2\2\u0146\u0147\3\2\2\2\u0147"+
		"\23\3\2\2\2\u0148\u014b\5\26\f\2\u0149\u014a\7\34\2\2\u014a\u014c\5.\30"+
		"\2\u014b\u0149\3\2\2\2\u014b\u014c\3\2\2\2\u014c\u014e\3\2\2\2\u014d\u014f"+
		"\5r:\2\u014e\u014d\3\2\2\2\u014e\u014f\3\2\2\2\u014f\u0151\3\2\2\2\u0150"+
		"\u0152\5p9\2\u0151\u0150\3\2\2\2\u0151\u0152\3\2\2\2\u0152\u0154\3\2\2"+
		"\2\u0153\u0155\5n8\2\u0154\u0153\3\2\2\2\u0154\u0155\3\2\2\2\u0155\u0157"+
		"\3\2\2\2\u0156\u0158\5X-\2\u0157\u0156\3\2\2\2\u0157\u0158\3\2\2\2\u0158"+
		"\u015a\3\2\2\2\u0159\u015b\5b\62\2\u015a\u0159\3\2\2\2\u015a\u015b\3\2"+
		"\2\2\u015b\u015d\3\2\2\2\u015c\u015e\5h\65\2\u015d\u015c\3\2\2\2\u015d"+
		"\u015e\3\2\2\2\u015e\u0160\3\2\2\2\u015f\u0161\5d\63\2\u0160\u015f\3\2"+
		"\2\2\u0160\u0161\3\2\2\2\u0161\u0163\3\2\2\2\u0162\u0164\5z>\2\u0163\u0162"+
		"\3\2\2\2\u0163\u0164\3\2\2\2\u0164\u0166\3\2\2\2\u0165\u0167\5\u0086D"+
		"\2\u0166\u0165\3\2\2\2\u0166\u0167\3\2\2\2\u0167\u0169\3\2\2\2\u0168\u016a"+
		"\5\u0090I\2\u0169\u0168\3\2\2\2\u0169\u016a\3\2\2\2\u016a\u016c\3\2\2"+
		"\2\u016b\u016d\5\u0098M\2\u016c\u016b\3\2\2\2\u016c\u016d\3\2\2\2\u016d"+
		"\u016f\3\2\2\2\u016e\u0170\5\u009cO\2\u016f\u016e\3\2\2\2\u016f\u0170"+
		"\3\2\2\2\u0170\u0172\3\2\2\2\u0171\u0173\5\u00a0Q\2\u0172\u0171\3\2\2"+
		"\2\u0172\u0173\3\2\2\2\u0173\25\3\2\2\2\u0174\u0175\7\6\2\2\u0175\u0176"+
		"\5\"\22\2\u0176\u0177\7\7\2\2\u0177\u0179\5\30\r\2\u0178\u017a\5(\25\2"+
		"\u0179\u0178\3\2\2\2\u0179\u017a\3\2\2\2\u017a\27\3\2\2\2\u017b\u017d"+
		"\t\2\2\2\u017c\u017b\3\2\2\2\u017d\u017e\3\2\2\2\u017e\u017c\3\2\2\2\u017e"+
		"\u017f\3\2\2\2\u017f\31\3\2\2\2\u0180\u0182\t\2\2\2\u0181\u0180\3\2\2"+
		"\2\u0182\u0183\3\2\2\2\u0183\u0181\3\2\2\2\u0183\u0184\3\2\2\2\u0184\33"+
		"\3\2\2\2\u0185\u0188\5\36\20\2\u0186\u0187\7\t\2\2\u0187\u0189\5 \21\2"+
		"\u0188\u0186\3\2\2\2\u0188\u0189\3\2\2\2\u0189\u018c\3\2\2\2\u018a\u018c"+
		"\7\64\2\2\u018b\u0185\3\2\2\2\u018b\u018a\3\2\2\2\u018c\35\3\2\2\2\u018d"+
		"\u0192\79\2\2\u018e\u018f\7\66\2\2\u018f\u0191\79\2\2\u0190\u018e\3\2"+
		"\2\2\u0191\u0194\3\2\2\2\u0192\u0190\3\2\2\2\u0192\u0193\3\2\2\2\u0193"+
		"\u0197\3\2\2\2\u0194\u0192\3\2\2\2\u0195\u0196\7\66\2\2\u0196\u0198\7"+
		"\64\2\2\u0197\u0195\3\2\2\2\u0197\u0198\3\2\2\2\u0198\37\3\2\2\2\u0199"+
		"\u019a\79\2\2\u019a!\3\2\2\2\u019b\u01a0\5\34\17\2\u019c\u019d\7\65\2"+
		"\2\u019d\u019f\5\34\17\2\u019e\u019c\3\2\2\2\u019f\u01a2\3\2\2\2\u01a0"+
		"\u019e\3\2\2\2\u01a0\u01a1\3\2\2\2\u01a1#\3\2\2\2\u01a2\u01a0\3\2\2\2"+
		"\u01a3\u01a4\7\7\2\2\u01a4\u01a5\5\32\16\2\u01a5%\3\2\2\2\u01a6\u01a7"+
		"\t\3\2\2\u01a7\'\3\2\2\2\u01a8\u01a9\7\b\2\2\u01a9\u01aa\5*\26\2\u01aa"+
		")\3\2\2\2\u01ab\u01b0\5\34\17\2\u01ac\u01ad\7\65\2\2\u01ad\u01af\5\34"+
		"\17\2\u01ae\u01ac\3\2\2\2\u01af\u01b2\3\2\2\2\u01b0\u01ae\3\2\2\2\u01b0"+
		"\u01b1\3\2\2\2\u01b1+\3\2\2\2\u01b2\u01b0\3\2\2\2\u01b3\u01b4\5\36\20"+
		"\2\u01b4-\3\2\2\2\u01b5\u01ba\5,\27\2\u01b6\u01b7\7\65\2\2\u01b7\u01b9"+
		"\5,\27\2\u01b8\u01b6\3\2\2\2\u01b9\u01bc\3\2\2\2\u01ba\u01b8\3\2\2\2\u01ba"+
		"\u01bb\3\2\2\2\u01bb/\3\2\2\2\u01bc\u01ba\3\2\2\2\u01bd\u01be\7\n\2\2"+
		"\u01be\61\3\2\2\2\u01bf\u01c0\7\13\2\2\u01c0\63\3\2\2\2\u01c1\u01c2\7"+
		"\63\2\2\u01c2\65\3\2\2\2\u01c3\u01c4\7\16\2\2\u01c4\u01c5\7\62\2\2\u01c5"+
		"\u01c6\5\64\33\2\u01c6\67\3\2\2\2\u01c7\u01c8\7\17\2\2\u01c89\3\2\2\2"+
		"\u01c9\u01ca\7\20\2\2\u01ca;\3\2\2\2\u01cb\u01cc\79\2\2\u01cc=\3\2\2\2"+
		"\u01cd\u01d2\5<\37\2\u01ce\u01cf\7\65\2\2\u01cf\u01d1\5<\37\2\u01d0\u01ce"+
		"\3\2\2\2\u01d1\u01d4\3\2\2\2\u01d2\u01d0\3\2\2\2\u01d2\u01d3\3\2\2\2\u01d3"+
		"?\3\2\2\2\u01d4\u01d2\3\2\2\2\u01d5\u01d6\7\21\2\2\u01d6\u01d7\5> \2\u01d7"+
		"A\3\2\2\2\u01d8\u01d9\79\2\2\u01d9C\3\2\2\2\u01da\u01df\5B\"\2\u01db\u01dc"+
		"\7\65\2\2\u01dc\u01de\5B\"\2\u01dd\u01db\3\2\2\2\u01de\u01e1\3\2\2\2\u01df"+
		"\u01dd\3\2\2\2\u01df\u01e0\3\2\2\2\u01e0E\3\2\2\2\u01e1\u01df\3\2\2\2"+
		"\u01e2\u01e3\7\22\2\2\u01e3\u01e4\5D#\2\u01e4\u01e5\7\5\2\2\u01e5\u01e6"+
		"\5P)\2\u01e6\u01e7\7\r\2\2\u01e7G\3\2\2\2\u01e8\u01e9\7\23\2\2\u01e9\u01ea"+
		"\5J&\2\u01eaI\3\2\2\2\u01eb\u01ec\t\4\2\2\u01ecK\3\2\2\2\u01ed\u01ee\7"+
		"/\2\2\u01ee\u01ef\7\62\2\2\u01ef\u01f0\5N(\2\u01f0M\3\2\2\2\u01f1\u01f2"+
		"\79\2\2\u01f2O\3\2\2\2\u01f3\u01f4\7\63\2\2\u01f4Q\3\2\2\2\u01f5\u01f6"+
		"\79\2\2\u01f6S\3\2\2\2\u01f7\u01fc\5R*\2\u01f8\u01f9\7\65\2\2\u01f9\u01fb"+
		"\5R*\2\u01fa\u01f8\3\2\2\2\u01fb\u01fe\3\2\2\2\u01fc\u01fa\3\2\2\2\u01fc"+
		"\u01fd\3\2\2\2\u01fdU\3\2\2\2\u01fe\u01fc\3\2\2\2\u01ff\u0200\7\f\2\2"+
		"\u0200\u0201\5T+\2\u0201\u0202\7\5\2\2\u0202\u0203\5P)\2\u0203\u0204\7"+
		"\r\2\2\u0204W\3\2\2\2\u0205\u0206\7\25\2\2\u0206\u0207\5Z.\2\u0207Y\3"+
		"\2\2\2\u0208\u0209\t\5\2\2\u0209[\3\2\2\2\u020a\u020d\7\63\2\2\u020b\u020c"+
		"\7\65\2\2\u020c\u020e\7\63\2\2\u020d\u020b\3\2\2\2\u020d\u020e\3\2\2\2"+
		"\u020e]\3\2\2\2\u020f\u0210\7\67\2\2\u0210\u0211\5\\/\2\u0211\u0212\7"+
		"8\2\2\u0212_\3\2\2\2\u0213\u0218\5^\60\2\u0214\u0215\7\65\2\2\u0215\u0217"+
		"\5^\60\2\u0216\u0214\3\2\2\2\u0217\u021a\3\2\2\2\u0218\u0216\3\2\2\2\u0218"+
		"\u0219\3\2\2\2\u0219a\3\2\2\2\u021a\u0218\3\2\2\2\u021b\u021c\7\26\2\2"+
		"\u021c\u021d\5`\61\2\u021dc\3\2\2\2\u021e\u021f\7$\2\2\u021f\u0220\5f"+
		"\64\2\u0220e\3\2\2\2\u0221\u0222\7\63\2\2\u0222g\3\2\2\2\u0223\u0224\7"+
		"\35\2\2\u0224\u0225\5j\66\2\u0225\u0226\7\36\2\2\u0226\u0227\5l\67\2\u0227"+
		"i\3\2\2\2\u0228\u0229\7\63\2\2\u0229k\3\2\2\2\u022a\u022b\7\63\2\2\u022b"+
		"m\3\2\2\2\u022c\u022d\7 \2\2\u022do\3\2\2\2\u022e\u022f\7\37\2\2\u022f"+
		"\u0230\5t;\2\u0230q\3\2\2\2\u0231\u0232\7\32\2\2\u0232s\3\2\2\2\u0233"+
		"\u0234\7!\2\2\u0234u\3\2\2\2\u0235\u0236\7\"\2\2\u0236\u0237\5x=\2\u0237"+
		"w\3\2\2\2\u0238\u0239\7\63\2\2\u0239y\3\2\2\2\u023a\u023b\7#\2\2\u023b"+
		"\u023f\5|?\2\u023c\u023e\5~@\2\u023d\u023c\3\2\2\2\u023e\u0241\3\2\2\2"+
		"\u023f\u023d\3\2\2\2\u023f\u0240\3\2\2\2\u0240{\3\2\2\2\u0241\u023f\3"+
		"\2\2\2\u0242\u0249\79\2\2\u0243\u0245\t\6\2\2\u0244\u0243\3\2\2\2\u0245"+
		"\u0246\3\2\2\2\u0246\u0244\3\2\2\2\u0246\u0247\3\2\2\2\u0247\u0249\3\2"+
		"\2\2\u0248\u0242\3\2\2\2\u0248\u0244\3\2\2\2\u0249}\3\2\2\2\u024a\u024b"+
		"\7\67\2\2\u024b\u0250\5\u0080A\2\u024c\u024d\7\65\2\2\u024d\u024f\5\u0080"+
		"A\2\u024e\u024c\3\2\2\2\u024f\u0252\3\2\2\2\u0250\u024e\3\2\2\2\u0250"+
		"\u0251\3\2\2\2\u0251\u0253\3\2\2\2\u0252\u0250\3\2\2\2\u0253\u0254\78"+
		"\2\2\u0254\177\3\2\2\2\u0255\u0256\5\u0082B\2\u0256\u0257\7\62\2\2\u0257"+
		"\u0258\5\u0084C\2\u0258\u0081\3\2\2\2\u0259\u0260\79\2\2\u025a\u025c\t"+
		"\6\2\2\u025b\u025a\3\2\2\2\u025c\u025d\3\2\2\2\u025d\u025b\3\2\2\2\u025d"+
		"\u025e\3\2\2\2\u025e\u0260\3\2\2\2\u025f\u0259\3\2\2\2\u025f\u025b\3\2"+
		"\2\2\u0260\u0083\3\2\2\2\u0261\u0269\79\2\2\u0262\u0264\t\6\2\2\u0263"+
		"\u0262\3\2\2\2\u0264\u0265\3\2\2\2\u0265\u0263\3\2\2\2\u0265\u0266\3\2"+
		"\2\2\u0266\u0269\3\2\2\2\u0267\u0269\7\63\2\2\u0268\u0261\3\2\2\2\u0268"+
		"\u0263\3\2\2\2\u0268\u0267\3\2\2\2\u0269\u0085\3\2\2\2\u026a\u026b\7\27"+
		"\2\2\u026b\u026c\7\67\2\2\u026c\u0271\5\u00acW\2\u026d\u026e\7\65\2\2"+
		"\u026e\u0270\5\u00acW\2\u026f\u026d\3\2\2\2\u0270\u0273\3\2\2\2\u0271"+
		"\u026f\3\2\2\2\u0271\u0272\3\2\2\2\u0272\u0274\3\2\2\2\u0273\u0271\3\2"+
		"\2\2\u0274\u0275\78\2\2\u0275\u0087\3\2\2\2\u0276\u0277\7\30\2\2\u0277"+
		"\u0278\7\67\2\2\u0278\u027d\5\u008aF\2\u0279\u027a\7\65\2\2\u027a\u027c"+
		"\5\u008aF\2\u027b\u0279\3\2\2\2\u027c\u027f\3\2\2\2\u027d\u027b\3\2\2"+
		"\2\u027d\u027e\3\2\2\2\u027e\u0280\3\2\2\2\u027f\u027d\3\2\2\2\u0280\u0281"+
		"\78\2\2\u0281\u0089\3\2\2\2\u0282\u0283\t\5\2\2\u0283\u008b\3\2\2\2\u0284"+
		"\u0285\7\31\2\2\u0285\u0286\7\62\2\2\u0286\u0287\5\u008eH\2\u0287\u008d"+
		"\3\2\2\2\u0288\u0289\7;\2\2\u0289\u008f\3\2\2\2\u028a\u028b\7%\2\2\u028b"+
		"\u028c\7\62\2\2\u028c\u028d\5\u0092J\2\u028d\u0091\3\2\2\2\u028e\u028f"+
		"\t\3\2\2\u028f\u0093\3\2\2\2\u0290\u0291\7\33\2\2\u0291\u0292\5\u0096"+
		"L\2\u0292\u0095\3\2\2\2\u0293\u0294\79\2\2\u0294\u0097\3\2\2\2\u0295\u0296"+
		"\7&\2\2\u0296\u0297\7\62\2\2\u0297\u0298\5\u009aN\2\u0298\u0099\3\2\2"+
		"\2\u0299\u029a\t\5\2\2\u029a\u009b\3\2\2\2\u029b\u029c\7\'\2\2\u029c\u029d"+
		"\7\62\2\2\u029d\u029e\5\u009eP\2\u029e\u009d\3\2\2\2\u029f\u02a0\t\5\2"+
		"\2\u02a0\u009f\3\2\2\2\u02a1\u02a2\7(\2\2\u02a2\u02a3\7\62\2\2\u02a3\u02a4"+
		"\5\u00a2R\2\u02a4\u00a1\3\2\2\2\u02a5\u02a6\t\7\2\2\u02a6\u00a3\3\2\2"+
		"\2\u02a7\u02a8\7*\2\2\u02a8\u02a9\7\62\2\2\u02a9\u02aa\5\u00a6T\2\u02aa"+
		"\u00a5\3\2\2\2\u02ab\u02ad\t\b\2\2\u02ac\u02ab\3\2\2\2\u02ad\u02ae\3\2"+
		"\2\2\u02ae\u02ac\3\2\2\2\u02ae\u02af\3\2\2\2\u02af\u02b6\3\2\2\2\u02b0"+
		"\u02b2\t\6\2\2\u02b1\u02b0\3\2\2\2\u02b2\u02b3\3\2\2\2\u02b3\u02b1\3\2"+
		"\2\2\u02b3\u02b4\3\2\2\2\u02b4\u02b6\3\2\2\2\u02b5\u02ac\3\2\2\2\u02b5"+
		"\u02b1\3\2\2\2\u02b6\u00a7\3\2\2\2\u02b7\u02b8\7)\2\2\u02b8\u02b9\7\62"+
		"\2\2\u02b9\u02ba\5\u00aaV\2\u02ba\u00a9\3\2\2\2\u02bb\u02bc\t\7\2\2\u02bc"+
		"\u00ab\3\2\2\2\u02bd\u02c2\5\u00aeX\2\u02be\u02bf\7\62\2\2\u02bf\u02c3"+
		"\5\u00b0Y\2\u02c0\u02c1\7\t\2\2\u02c1\u02c3\5\u00b0Y\2\u02c2\u02be\3\2"+
		"\2\2\u02c2\u02c0\3\2\2\2\u02c2\u02c3\3\2\2\2\u02c3\u00ad\3\2\2\2\u02c4"+
		"\u02c6\t\b\2\2\u02c5\u02c4\3\2\2\2\u02c6\u02c7\3\2\2\2\u02c7\u02c5\3\2"+
		"\2\2\u02c7\u02c8\3\2\2\2\u02c8\u02cf\3\2\2\2\u02c9\u02cb\t\6\2\2\u02ca"+
		"\u02c9\3\2\2\2\u02cb\u02cc\3\2\2\2\u02cc\u02ca\3\2\2\2\u02cc\u02cd\3\2"+
		"\2\2\u02cd\u02cf\3\2\2\2\u02ce\u02c5\3\2\2\2\u02ce\u02ca\3\2\2\2\u02cf"+
		"\u00af\3\2\2\2\u02d0\u02d8\79\2\2\u02d1\u02d3\t\6\2\2\u02d2\u02d1\3\2"+
		"\2\2\u02d3\u02d4\3\2\2\2\u02d4\u02d2\3\2\2\2\u02d4\u02d5\3\2\2\2\u02d5"+
		"\u02d8\3\2\2\2\u02d6\u02d8\7\63\2\2\u02d7\u02d0\3\2\2\2\u02d7\u02d2\3"+
		"\2\2\2\u02d7\u02d6\3\2\2\2\u02d8\u00b1\3\2\2\2\u02d9\u02da\7\61\2\2\u02da"+
		"\u02db\7\62\2\2\u02db\u02dc\5\u00b4[\2\u02dc\u00b3\3\2\2\2\u02dd\u02de"+
		"\7\63\2\2\u02de\u00b5\3\2\2\2\u02df\u02e0\7\60\2\2\u02e0\u02e1\7\62\2"+
		"\2\u02e1\u02e2\5\u00b8]\2\u02e2\u00b7\3\2\2\2\u02e3\u02eb\79\2\2\u02e4"+
		"\u02e6\t\6\2\2\u02e5\u02e4\3\2\2\2\u02e6\u02e7\3\2\2\2\u02e7\u02e5\3\2"+
		"\2\2\u02e7\u02e8\3\2\2\2\u02e8\u02eb\3\2\2\2\u02e9\u02eb\7\63\2\2\u02ea"+
		"\u02e3\3\2\2\2\u02ea\u02e5\3\2\2\2\u02ea\u02e9\3\2\2\2\u02eb\u02ec\3\2"+
		"\2\2\u02ec\u02ea\3\2\2\2\u02ec\u02ed\3\2\2\2\u02ed\u00b9\3\2\2\2\u02ee"+
		"\u02ef\7+\2\2\u02ef\u02f0\7\62\2\2\u02f0\u02f1\5\u00bc_\2\u02f1\u00bb"+
		"\3\2\2\2\u02f2\u02f3\79\2\2\u02f3\u00bd\3\2\2\2\u02f4\u02f5\7,\2\2\u02f5"+
		"\u02f6\7\62\2\2\u02f6\u02f7\5\u00c0a\2\u02f7\u00bf\3\2\2\2\u02f8\u02f9"+
		"\7\63\2\2\u02f9\u00c1\3\2\2\2\u02fa\u02fb\7-\2\2\u02fb\u02fc\7\62\2\2"+
		"\u02fc\u02fd\5\u00c4c\2\u02fd\u00c3\3\2\2\2\u02fe\u02ff\79\2\2\u02ff\u00c5"+
		"\3\2\2\2\u0300\u0301\7.\2\2\u0301\u0302\7\62\2\2\u0302\u0303\5\u00c8e"+
		"\2\u0303\u00c7\3\2\2\2\u0304\u0305\79\2\2\u0305\u00c9\3\2\2\2W\u00cc\u00e0"+
		"\u00e8\u00eb\u00ef\u00f2\u00f5\u00f8\u00fb\u00fe\u0101\u0104\u0107\u010a"+
		"\u010d\u0110\u0113\u0116\u0119\u011c\u011f\u0122\u0125\u0128\u012b\u012e"+
		"\u0131\u0134\u0137\u013a\u013d\u0140\u0143\u0146\u014b\u014e\u0151\u0154"+
		"\u0157\u015a\u015d\u0160\u0163\u0166\u0169\u016c\u016f\u0172\u0179\u017e"+
		"\u0183\u0188\u018b\u0192\u0197\u01a0\u01b0\u01ba\u01d2\u01df\u01fc\u020d"+
		"\u0218\u023f\u0246\u0248\u0250\u025d\u025f\u0265\u0268\u0271\u027d\u02ae"+
		"\u02b3\u02b5\u02c2\u02c7\u02cc\u02ce\u02d4\u02d7\u02e7\u02ea\u02ec";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}