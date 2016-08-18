// Generated from ConnectorLexer.g4 by ANTLR 4.5.3
package com.datamountaineer.connector.config.antlr4;

 
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ConnectorLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		INSERT=1, UPSERT=2, INTO=3, SELECT=4, FROM=5, IGNORE=6, AS=7, AUTOCREATE=8, 
		AUTOEVOLVE=9, CLUSTERBY=10, BUCKETS=11, BATCH=12, CAPITALIZE=13, PARTITIONBY=14, 
		DISTRIBUTEBY=15, TIMESTAMP=16, STOREDAS=17, SYS_TIME=18, WITHGROUP=19, 
		WITHOFFSET=20, PK=21, SAMPLE=22, EVERY=23, EQUAL=24, INT=25, ASTERISK=26, 
		COMMA=27, DOT=28, LEFT_PARAN=29, RIGHT_PARAN=30, ID=31, TOPICNAME=32, 
		NEWLINE=33, WS=34;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "PARTITIONBY", 
		"DISTRIBUTEBY", "TIMESTAMP", "STOREDAS", "SYS_TIME", "WITHGROUP", "WITHOFFSET", 
		"PK", "SAMPLE", "EVERY", "EQUAL", "INT", "ASTERISK", "COMMA", "DOT", "LEFT_PARAN", 
		"RIGHT_PARAN", "ID", "TOPICNAME", "NEWLINE", "WS"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		"'='", null, "'*'", "','", "'.'", "'('", "')'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "PARTITIONBY", 
		"DISTRIBUTEBY", "TIMESTAMP", "STOREDAS", "SYS_TIME", "WITHGROUP", "WITHOFFSET", 
		"PK", "SAMPLE", "EVERY", "EQUAL", "INT", "ASTERISK", "COMMA", "DOT", "LEFT_PARAN", 
		"RIGHT_PARAN", "ID", "TOPICNAME", "NEWLINE", "WS"
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


	public ConnectorLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "ConnectorLexer.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2$\u01f4\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\5\2"+
		"T\n\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3b\n\3\3\4\3\4"+
		"\3\4\3\4\3\4\3\4\3\4\3\4\5\4l\n\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5"+
		"\3\5\3\5\3\5\5\5z\n\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u0084\n\6\3"+
		"\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u0092\n\7\3\b\3\b\3"+
		"\b\3\b\5\b\u0098\n\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u00ae\n\t\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u00c4\n\n\3"+
		"\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3"+
		"\13\3\13\3\13\3\13\5\13\u00d8\n\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f"+
		"\3\f\3\f\3\f\3\f\3\f\5\f\u00e8\n\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r"+
		"\3\r\5\r\u00f4\n\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3"+
		"\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\5\16\u010a\n\16\3\17"+
		"\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
		"\3\17\3\17\3\17\3\17\3\17\3\17\3\17\5\17\u0122\n\17\3\20\3\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\5\20\u013c\n\20\3\21\3\21\3\21\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\5\21\u0158\n\21\3\22\3\22\3\22\3\22"+
		"\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\5\22\u016a"+
		"\n\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23"+
		"\3\23\3\23\3\23\3\23\3\23\3\23\3\23\5\23\u0180\n\23\3\24\3\24\3\24\3\24"+
		"\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24"+
		"\5\24\u0194\n\24\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25"+
		"\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\3\25\5\25\u01aa\n\25\3\26\3\26"+
		"\3\26\3\26\5\26\u01b0\n\26\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27\3\27"+
		"\3\27\3\27\3\27\5\27\u01be\n\27\3\30\3\30\3\30\3\30\3\30\3\30\3\30\3\30"+
		"\3\30\3\30\5\30\u01ca\n\30\3\31\3\31\3\32\6\32\u01cf\n\32\r\32\16\32\u01d0"+
		"\3\33\3\33\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \6 \u01de\n \r \16"+
		" \u01df\3!\6!\u01e3\n!\r!\16!\u01e4\3\"\5\"\u01e8\n\"\3\"\3\"\3\"\3\""+
		"\3#\6#\u01ef\n#\r#\16#\u01f0\3#\3#\2\2$\3\3\5\4\7\5\t\6\13\7\r\b\17\t"+
		"\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27"+
		"-\30/\31\61\32\63\33\65\34\67\359\36;\37= ?!A\"C#E$\3\2\5\6\2\62;C\\a"+
		"ac|\b\2--/\60\62;C\\aac|\5\2\13\f\17\17\"\"\u020f\2\3\3\2\2\2\2\5\3\2"+
		"\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21"+
		"\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2"+
		"\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3"+
		"\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3"+
		"\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3"+
		"\2\2\2\2A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\3S\3\2\2\2\5a\3\2\2\2\7k\3\2\2"+
		"\2\ty\3\2\2\2\13\u0083\3\2\2\2\r\u0091\3\2\2\2\17\u0097\3\2\2\2\21\u00ad"+
		"\3\2\2\2\23\u00c3\3\2\2\2\25\u00d7\3\2\2\2\27\u00e7\3\2\2\2\31\u00f3\3"+
		"\2\2\2\33\u0109\3\2\2\2\35\u0121\3\2\2\2\37\u013b\3\2\2\2!\u0157\3\2\2"+
		"\2#\u0169\3\2\2\2%\u017f\3\2\2\2\'\u0193\3\2\2\2)\u01a9\3\2\2\2+\u01af"+
		"\3\2\2\2-\u01bd\3\2\2\2/\u01c9\3\2\2\2\61\u01cb\3\2\2\2\63\u01ce\3\2\2"+
		"\2\65\u01d2\3\2\2\2\67\u01d4\3\2\2\29\u01d6\3\2\2\2;\u01d8\3\2\2\2=\u01da"+
		"\3\2\2\2?\u01dd\3\2\2\2A\u01e2\3\2\2\2C\u01e7\3\2\2\2E\u01ee\3\2\2\2G"+
		"H\7k\2\2HI\7p\2\2IJ\7u\2\2JK\7g\2\2KL\7t\2\2LT\7v\2\2MN\7K\2\2NO\7P\2"+
		"\2OP\7U\2\2PQ\7G\2\2QR\7T\2\2RT\7V\2\2SG\3\2\2\2SM\3\2\2\2T\4\3\2\2\2"+
		"UV\7w\2\2VW\7r\2\2WX\7u\2\2XY\7g\2\2YZ\7t\2\2Zb\7v\2\2[\\\7W\2\2\\]\7"+
		"R\2\2]^\7U\2\2^_\7G\2\2_`\7T\2\2`b\7V\2\2aU\3\2\2\2a[\3\2\2\2b\6\3\2\2"+
		"\2cd\7k\2\2de\7p\2\2ef\7v\2\2fl\7q\2\2gh\7K\2\2hi\7P\2\2ij\7V\2\2jl\7"+
		"Q\2\2kc\3\2\2\2kg\3\2\2\2l\b\3\2\2\2mn\7u\2\2no\7g\2\2op\7n\2\2pq\7g\2"+
		"\2qr\7e\2\2rz\7v\2\2st\7U\2\2tu\7G\2\2uv\7N\2\2vw\7G\2\2wx\7E\2\2xz\7"+
		"V\2\2ym\3\2\2\2ys\3\2\2\2z\n\3\2\2\2{|\7h\2\2|}\7t\2\2}~\7q\2\2~\u0084"+
		"\7o\2\2\177\u0080\7H\2\2\u0080\u0081\7T\2\2\u0081\u0082\7Q\2\2\u0082\u0084"+
		"\7O\2\2\u0083{\3\2\2\2\u0083\177\3\2\2\2\u0084\f\3\2\2\2\u0085\u0086\7"+
		"k\2\2\u0086\u0087\7i\2\2\u0087\u0088\7p\2\2\u0088\u0089\7q\2\2\u0089\u008a"+
		"\7t\2\2\u008a\u0092\7g\2\2\u008b\u008c\7K\2\2\u008c\u008d\7I\2\2\u008d"+
		"\u008e\7P\2\2\u008e\u008f\7Q\2\2\u008f\u0090\7T\2\2\u0090\u0092\7G\2\2"+
		"\u0091\u0085\3\2\2\2\u0091\u008b\3\2\2\2\u0092\16\3\2\2\2\u0093\u0094"+
		"\7c\2\2\u0094\u0098\7u\2\2\u0095\u0096\7C\2\2\u0096\u0098\7U\2\2\u0097"+
		"\u0093\3\2\2\2\u0097\u0095\3\2\2\2\u0098\20\3\2\2\2\u0099\u009a\7c\2\2"+
		"\u009a\u009b\7w\2\2\u009b\u009c\7v\2\2\u009c\u009d\7q\2\2\u009d\u009e"+
		"\7e\2\2\u009e\u009f\7t\2\2\u009f\u00a0\7g\2\2\u00a0\u00a1\7c\2\2\u00a1"+
		"\u00a2\7v\2\2\u00a2\u00ae\7g\2\2\u00a3\u00a4\7C\2\2\u00a4\u00a5\7W\2\2"+
		"\u00a5\u00a6\7V\2\2\u00a6\u00a7\7Q\2\2\u00a7\u00a8\7E\2\2\u00a8\u00a9"+
		"\7T\2\2\u00a9\u00aa\7G\2\2\u00aa\u00ab\7C\2\2\u00ab\u00ac\7V\2\2\u00ac"+
		"\u00ae\7G\2\2\u00ad\u0099\3\2\2\2\u00ad\u00a3\3\2\2\2\u00ae\22\3\2\2\2"+
		"\u00af\u00b0\7c\2\2\u00b0\u00b1\7w\2\2\u00b1\u00b2\7v\2\2\u00b2\u00b3"+
		"\7q\2\2\u00b3\u00b4\7g\2\2\u00b4\u00b5\7x\2\2\u00b5\u00b6\7q\2\2\u00b6"+
		"\u00b7\7n\2\2\u00b7\u00b8\7x\2\2\u00b8\u00c4\7g\2\2\u00b9\u00ba\7C\2\2"+
		"\u00ba\u00bb\7W\2\2\u00bb\u00bc\7V\2\2\u00bc\u00bd\7Q\2\2\u00bd\u00be"+
		"\7G\2\2\u00be\u00bf\7X\2\2\u00bf\u00c0\7Q\2\2\u00c0\u00c1\7N\2\2\u00c1"+
		"\u00c2\7X\2\2\u00c2\u00c4\7G\2\2\u00c3\u00af\3\2\2\2\u00c3\u00b9\3\2\2"+
		"\2\u00c4\24\3\2\2\2\u00c5\u00c6\7e\2\2\u00c6\u00c7\7n\2\2\u00c7\u00c8"+
		"\7w\2\2\u00c8\u00c9\7u\2\2\u00c9\u00ca\7v\2\2\u00ca\u00cb\7g\2\2\u00cb"+
		"\u00cc\7t\2\2\u00cc\u00cd\7d\2\2\u00cd\u00d8\7{\2\2\u00ce\u00cf\7E\2\2"+
		"\u00cf\u00d0\7N\2\2\u00d0\u00d1\7W\2\2\u00d1\u00d2\7U\2\2\u00d2\u00d3"+
		"\7V\2\2\u00d3\u00d4\7G\2\2\u00d4\u00d5\7T\2\2\u00d5\u00d6\7D\2\2\u00d6"+
		"\u00d8\7[\2\2\u00d7\u00c5\3\2\2\2\u00d7\u00ce\3\2\2\2\u00d8\26\3\2\2\2"+
		"\u00d9\u00da\7d\2\2\u00da\u00db\7w\2\2\u00db\u00dc\7e\2\2\u00dc\u00dd"+
		"\7m\2\2\u00dd\u00de\7g\2\2\u00de\u00df\7v\2\2\u00df\u00e8\7u\2\2\u00e0"+
		"\u00e1\7D\2\2\u00e1\u00e2\7W\2\2\u00e2\u00e3\7E\2\2\u00e3\u00e4\7M\2\2"+
		"\u00e4\u00e5\7G\2\2\u00e5\u00e6\7V\2\2\u00e6\u00e8\7U\2\2\u00e7\u00d9"+
		"\3\2\2\2\u00e7\u00e0\3\2\2\2\u00e8\30\3\2\2\2\u00e9\u00ea\7d\2\2\u00ea"+
		"\u00eb\7c\2\2\u00eb\u00ec\7v\2\2\u00ec\u00ed\7e\2\2\u00ed\u00f4\7j\2\2"+
		"\u00ee\u00ef\7D\2\2\u00ef\u00f0\7C\2\2\u00f0\u00f1\7V\2\2\u00f1\u00f2"+
		"\7E\2\2\u00f2\u00f4\7J\2\2\u00f3\u00e9\3\2\2\2\u00f3\u00ee\3\2\2\2\u00f4"+
		"\32\3\2\2\2\u00f5\u00f6\7e\2\2\u00f6\u00f7\7c\2\2\u00f7\u00f8\7r\2\2\u00f8"+
		"\u00f9\7k\2\2\u00f9\u00fa\7v\2\2\u00fa\u00fb\7c\2\2\u00fb\u00fc\7n\2\2"+
		"\u00fc\u00fd\7k\2\2\u00fd\u00fe\7|\2\2\u00fe\u010a\7g\2\2\u00ff\u0100"+
		"\7E\2\2\u0100\u0101\7C\2\2\u0101\u0102\7R\2\2\u0102\u0103\7K\2\2\u0103"+
		"\u0104\7V\2\2\u0104\u0105\7C\2\2\u0105\u0106\7N\2\2\u0106\u0107\7K\2\2"+
		"\u0107\u0108\7\\\2\2\u0108\u010a\7G\2\2\u0109\u00f5\3\2\2\2\u0109\u00ff"+
		"\3\2\2\2\u010a\34\3\2\2\2\u010b\u010c\7r\2\2\u010c\u010d\7c\2\2\u010d"+
		"\u010e\7t\2\2\u010e\u010f\7v\2\2\u010f\u0110\7k\2\2\u0110\u0111\7v\2\2"+
		"\u0111\u0112\7k\2\2\u0112\u0113\7q\2\2\u0113\u0114\7p\2\2\u0114\u0115"+
		"\7d\2\2\u0115\u0122\7{\2\2\u0116\u0117\7R\2\2\u0117\u0118\7C\2\2\u0118"+
		"\u0119\7T\2\2\u0119\u011a\7V\2\2\u011a\u011b\7K\2\2\u011b\u011c\7V\2\2"+
		"\u011c\u011d\7K\2\2\u011d\u011e\7Q\2\2\u011e\u011f\7P\2\2\u011f\u0120"+
		"\7D\2\2\u0120\u0122\7[\2\2\u0121\u010b\3\2\2\2\u0121\u0116\3\2\2\2\u0122"+
		"\36\3\2\2\2\u0123\u0124\7f\2\2\u0124\u0125\7k\2\2\u0125\u0126\7u\2\2\u0126"+
		"\u0127\7v\2\2\u0127\u0128\7t\2\2\u0128\u0129\7k\2\2\u0129\u012a\7d\2\2"+
		"\u012a\u012b\7w\2\2\u012b\u012c\7v\2\2\u012c\u012d\7g\2\2\u012d\u012e"+
		"\7d\2\2\u012e\u013c\7{\2\2\u012f\u0130\7F\2\2\u0130\u0131\7K\2\2\u0131"+
		"\u0132\7U\2\2\u0132\u0133\7V\2\2\u0133\u0134\7T\2\2\u0134\u0135\7K\2\2"+
		"\u0135\u0136\7D\2\2\u0136\u0137\7W\2\2\u0137\u0138\7V\2\2\u0138\u0139"+
		"\7G\2\2\u0139\u013a\7D\2\2\u013a\u013c\7[\2\2\u013b\u0123\3\2\2\2\u013b"+
		"\u012f\3\2\2\2\u013c \3\2\2\2\u013d\u013e\7y\2\2\u013e\u013f\7k\2\2\u013f"+
		"\u0140\7v\2\2\u0140\u0141\7j\2\2\u0141\u0142\7v\2\2\u0142\u0143\7k\2\2"+
		"\u0143\u0144\7o\2\2\u0144\u0145\7g\2\2\u0145\u0146\7u\2\2\u0146\u0147"+
		"\7v\2\2\u0147\u0148\7c\2\2\u0148\u0149\7o\2\2\u0149\u0158\7r\2\2\u014a"+
		"\u014b\7Y\2\2\u014b\u014c\7K\2\2\u014c\u014d\7V\2\2\u014d\u014e\7J\2\2"+
		"\u014e\u014f\7V\2\2\u014f\u0150\7K\2\2\u0150\u0151\7O\2\2\u0151\u0152"+
		"\7G\2\2\u0152\u0153\7U\2\2\u0153\u0154\7V\2\2\u0154\u0155\7C\2\2\u0155"+
		"\u0156\7O\2\2\u0156\u0158\7R\2\2\u0157\u013d\3\2\2\2\u0157\u014a\3\2\2"+
		"\2\u0158\"\3\2\2\2\u0159\u015a\7u\2\2\u015a\u015b\7v\2\2\u015b\u015c\7"+
		"q\2\2\u015c\u015d\7t\2\2\u015d\u015e\7g\2\2\u015e\u015f\7f\2\2\u015f\u0160"+
		"\7c\2\2\u0160\u016a\7u\2\2\u0161\u0162\7U\2\2\u0162\u0163\7V\2\2\u0163"+
		"\u0164\7Q\2\2\u0164\u0165\7T\2\2\u0165\u0166\7G\2\2\u0166\u0167\7F\2\2"+
		"\u0167\u0168\7C\2\2\u0168\u016a\7U\2\2\u0169\u0159\3\2\2\2\u0169\u0161"+
		"\3\2\2\2\u016a$\3\2\2\2\u016b\u016c\7u\2\2\u016c\u016d\7{\2\2\u016d\u016e"+
		"\7u\2\2\u016e\u016f\7a\2\2\u016f\u0170\7v\2\2\u0170\u0171\7k\2\2\u0171"+
		"\u0172\7o\2\2\u0172\u0173\7g\2\2\u0173\u0174\7*\2\2\u0174\u0180\7+\2\2"+
		"\u0175\u0176\7U\2\2\u0176\u0177\7[\2\2\u0177\u0178\7U\2\2\u0178\u0179"+
		"\7a\2\2\u0179\u017a\7V\2\2\u017a\u017b\7K\2\2\u017b\u017c\7O\2\2\u017c"+
		"\u017d\7G\2\2\u017d\u017e\7*\2\2\u017e\u0180\7+\2\2\u017f\u016b\3\2\2"+
		"\2\u017f\u0175\3\2\2\2\u0180&\3\2\2\2\u0181\u0182\7y\2\2\u0182\u0183\7"+
		"k\2\2\u0183\u0184\7v\2\2\u0184\u0185\7j\2\2\u0185\u0186\7i\2\2\u0186\u0187"+
		"\7t\2\2\u0187\u0188\7q\2\2\u0188\u0189\7w\2\2\u0189\u0194\7r\2\2\u018a"+
		"\u018b\7Y\2\2\u018b\u018c\7K\2\2\u018c\u018d\7V\2\2\u018d\u018e\7J\2\2"+
		"\u018e\u018f\7I\2\2\u018f\u0190\7T\2\2\u0190\u0191\7Q\2\2\u0191\u0192"+
		"\7W\2\2\u0192\u0194\7R\2\2\u0193\u0181\3\2\2\2\u0193\u018a\3\2\2\2\u0194"+
		"(\3\2\2\2\u0195\u0196\7y\2\2\u0196\u0197\7k\2\2\u0197\u0198\7v\2\2\u0198"+
		"\u0199\7j\2\2\u0199\u019a\7q\2\2\u019a\u019b\7h\2\2\u019b\u019c\7h\2\2"+
		"\u019c\u019d\7u\2\2\u019d\u019e\7g\2\2\u019e\u01aa\7v\2\2\u019f\u01a0"+
		"\7Y\2\2\u01a0\u01a1\7K\2\2\u01a1\u01a2\7V\2\2\u01a2\u01a3\7J\2\2\u01a3"+
		"\u01a4\7Q\2\2\u01a4\u01a5\7H\2\2\u01a5\u01a6\7H\2\2\u01a6\u01a7\7U\2\2"+
		"\u01a7\u01a8\7G\2\2\u01a8\u01aa\7V\2\2\u01a9\u0195\3\2\2\2\u01a9\u019f"+
		"\3\2\2\2\u01aa*\3\2\2\2\u01ab\u01ac\7r\2\2\u01ac\u01b0\7m\2\2\u01ad\u01ae"+
		"\7R\2\2\u01ae\u01b0\7M\2\2\u01af\u01ab\3\2\2\2\u01af\u01ad\3\2\2\2\u01b0"+
		",\3\2\2\2\u01b1\u01b2\7u\2\2\u01b2\u01b3\7c\2\2\u01b3\u01b4\7o\2\2\u01b4"+
		"\u01b5\7r\2\2\u01b5\u01b6\7n\2\2\u01b6\u01be\7g\2\2\u01b7\u01b8\7U\2\2"+
		"\u01b8\u01b9\7C\2\2\u01b9\u01ba\7O\2\2\u01ba\u01bb\7R\2\2\u01bb\u01bc"+
		"\7N\2\2\u01bc\u01be\7G\2\2\u01bd\u01b1\3\2\2\2\u01bd\u01b7\3\2\2\2\u01be"+
		".\3\2\2\2\u01bf\u01c0\7g\2\2\u01c0\u01c1\7x\2\2\u01c1\u01c2\7g\2\2\u01c2"+
		"\u01c3\7t\2\2\u01c3\u01ca\7{\2\2\u01c4\u01c5\7G\2\2\u01c5\u01c6\7X\2\2"+
		"\u01c6\u01c7\7G\2\2\u01c7\u01c8\7T\2\2\u01c8\u01ca\7[\2\2\u01c9\u01bf"+
		"\3\2\2\2\u01c9\u01c4\3\2\2\2\u01ca\60\3\2\2\2\u01cb\u01cc\7?\2\2\u01cc"+
		"\62\3\2\2\2\u01cd\u01cf\4\62;\2\u01ce\u01cd\3\2\2\2\u01cf\u01d0\3\2\2"+
		"\2\u01d0\u01ce\3\2\2\2\u01d0\u01d1\3\2\2\2\u01d1\64\3\2\2\2\u01d2\u01d3"+
		"\7,\2\2\u01d3\66\3\2\2\2\u01d4\u01d5\7.\2\2\u01d58\3\2\2\2\u01d6\u01d7"+
		"\7\60\2\2\u01d7:\3\2\2\2\u01d8\u01d9\7*\2\2\u01d9<\3\2\2\2\u01da\u01db"+
		"\7+\2\2\u01db>\3\2\2\2\u01dc\u01de\t\2\2\2\u01dd\u01dc\3\2\2\2\u01de\u01df"+
		"\3\2\2\2\u01df\u01dd\3\2\2\2\u01df\u01e0\3\2\2\2\u01e0@\3\2\2\2\u01e1"+
		"\u01e3\t\3\2\2\u01e2\u01e1\3\2\2\2\u01e3\u01e4\3\2\2\2\u01e4\u01e2\3\2"+
		"\2\2\u01e4\u01e5\3\2\2\2\u01e5B\3\2\2\2\u01e6\u01e8\7\17\2\2\u01e7\u01e6"+
		"\3\2\2\2\u01e7\u01e8\3\2\2\2\u01e8\u01e9\3\2\2\2\u01e9\u01ea\7\f\2\2\u01ea"+
		"\u01eb\3\2\2\2\u01eb\u01ec\b\"\2\2\u01ecD\3\2\2\2\u01ed\u01ef\t\4\2\2"+
		"\u01ee\u01ed\3\2\2\2\u01ef\u01f0\3\2\2\2\u01f0\u01ee\3\2\2\2\u01f0\u01f1"+
		"\3\2\2\2\u01f1\u01f2\3\2\2\2\u01f2\u01f3\b#\2\2\u01f3F\3\2\2\2\37\2Sa"+
		"ky\u0083\u0091\u0097\u00ad\u00c3\u00d7\u00e7\u00f3\u0109\u0121\u013b\u0157"+
		"\u0169\u017f\u0193\u01a9\u01af\u01bd\u01c9\u01d0\u01df\u01e4\u01e7\u01f0"+
		"\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}