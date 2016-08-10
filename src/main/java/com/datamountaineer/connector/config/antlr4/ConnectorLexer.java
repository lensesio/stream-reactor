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
		DISTRIBUTEBY=15, TIMESTAMP=16, SYS_TIME=17, PK=18, INT=19, ASTERISK=20, 
		COMMA=21, DOT=22, ID=23, TOPICNAME=24, NEWLINE=25, WS=26, EQUAL=27;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "PARTITIONBY", 
		"DISTRIBUTEBY", "TIMESTAMP", "SYS_TIME", "PK", "INT", "ASTERISK", "COMMA", 
		"DOT", "ID", "TOPICNAME", "NEWLINE", "WS", "EQUAL"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, "'*'", "','", "'.'", null, 
		null, null, null, "'='"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "PARTITIONBY", 
		"DISTRIBUTEBY", "TIMESTAMP", "SYS_TIME", "PK", "INT", "ASTERISK", "COMMA", 
		"DOT", "ID", "TOPICNAME", "NEWLINE", "WS", "EQUAL"
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\35\u018c\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\4\33\t\33\4\34\t\34\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3"+
		"\2\3\2\3\2\3\2\5\2F\n\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\5\3T\n\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4^\n\4\3\5\3\5\3\5\3\5\3"+
		"\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5l\n\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3"+
		"\6\5\6v\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u0084"+
		"\n\7\3\b\3\b\3\b\3\b\5\b\u008a\n\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u00a0\n\t\3\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\5"+
		"\n\u00b6\n\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3"+
		"\13\3\13\3\13\3\13\3\13\3\13\3\13\5\13\u00ca\n\13\3\f\3\f\3\f\3\f\3\f"+
		"\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u00da\n\f\3\r\3\r\3\r\3\r\3\r"+
		"\3\r\3\r\3\r\3\r\3\r\5\r\u00e6\n\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16"+
		"\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\5\16"+
		"\u00fc\n\16\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
		"\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\5\17\u0114\n\17\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\5\20\u012e\n\20\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\5\21\u014a\n\21\3\22"+
		"\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"+
		"\3\22\3\22\3\22\3\22\3\22\5\22\u0160\n\22\3\23\3\23\3\23\3\23\5\23\u0166"+
		"\n\23\3\24\6\24\u0169\n\24\r\24\16\24\u016a\3\25\3\25\3\26\3\26\3\27\3"+
		"\27\3\30\6\30\u0174\n\30\r\30\16\30\u0175\3\31\6\31\u0179\n\31\r\31\16"+
		"\31\u017a\3\32\5\32\u017e\n\32\3\32\3\32\3\32\3\32\3\33\6\33\u0185\n\33"+
		"\r\33\16\33\u0186\3\33\3\33\3\34\3\34\2\2\35\3\3\5\4\7\5\t\6\13\7\r\b"+
		"\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26"+
		"+\27-\30/\31\61\32\63\33\65\34\67\35\3\2\5\6\2\62;C\\aac|\b\2--/\60\62"+
		";C\\aac|\5\2\13\f\17\17\"\"\u01a2\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2"+
		"\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3"+
		"\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2"+
		"\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2"+
		"\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2"+
		"\2\2\2\67\3\2\2\2\3E\3\2\2\2\5S\3\2\2\2\7]\3\2\2\2\tk\3\2\2\2\13u\3\2"+
		"\2\2\r\u0083\3\2\2\2\17\u0089\3\2\2\2\21\u009f\3\2\2\2\23\u00b5\3\2\2"+
		"\2\25\u00c9\3\2\2\2\27\u00d9\3\2\2\2\31\u00e5\3\2\2\2\33\u00fb\3\2\2\2"+
		"\35\u0113\3\2\2\2\37\u012d\3\2\2\2!\u0149\3\2\2\2#\u015f\3\2\2\2%\u0165"+
		"\3\2\2\2\'\u0168\3\2\2\2)\u016c\3\2\2\2+\u016e\3\2\2\2-\u0170\3\2\2\2"+
		"/\u0173\3\2\2\2\61\u0178\3\2\2\2\63\u017d\3\2\2\2\65\u0184\3\2\2\2\67"+
		"\u018a\3\2\2\29:\7k\2\2:;\7p\2\2;<\7u\2\2<=\7g\2\2=>\7t\2\2>F\7v\2\2?"+
		"@\7K\2\2@A\7P\2\2AB\7U\2\2BC\7G\2\2CD\7T\2\2DF\7V\2\2E9\3\2\2\2E?\3\2"+
		"\2\2F\4\3\2\2\2GH\7w\2\2HI\7r\2\2IJ\7u\2\2JK\7g\2\2KL\7t\2\2LT\7v\2\2"+
		"MN\7W\2\2NO\7R\2\2OP\7U\2\2PQ\7G\2\2QR\7T\2\2RT\7V\2\2SG\3\2\2\2SM\3\2"+
		"\2\2T\6\3\2\2\2UV\7k\2\2VW\7p\2\2WX\7v\2\2X^\7q\2\2YZ\7K\2\2Z[\7P\2\2"+
		"[\\\7V\2\2\\^\7Q\2\2]U\3\2\2\2]Y\3\2\2\2^\b\3\2\2\2_`\7u\2\2`a\7g\2\2"+
		"ab\7n\2\2bc\7g\2\2cd\7e\2\2dl\7v\2\2ef\7U\2\2fg\7G\2\2gh\7N\2\2hi\7G\2"+
		"\2ij\7E\2\2jl\7V\2\2k_\3\2\2\2ke\3\2\2\2l\n\3\2\2\2mn\7h\2\2no\7t\2\2"+
		"op\7q\2\2pv\7o\2\2qr\7H\2\2rs\7T\2\2st\7Q\2\2tv\7O\2\2um\3\2\2\2uq\3\2"+
		"\2\2v\f\3\2\2\2wx\7k\2\2xy\7i\2\2yz\7p\2\2z{\7q\2\2{|\7t\2\2|\u0084\7"+
		"g\2\2}~\7K\2\2~\177\7I\2\2\177\u0080\7P\2\2\u0080\u0081\7Q\2\2\u0081\u0082"+
		"\7T\2\2\u0082\u0084\7G\2\2\u0083w\3\2\2\2\u0083}\3\2\2\2\u0084\16\3\2"+
		"\2\2\u0085\u0086\7c\2\2\u0086\u008a\7u\2\2\u0087\u0088\7C\2\2\u0088\u008a"+
		"\7U\2\2\u0089\u0085\3\2\2\2\u0089\u0087\3\2\2\2\u008a\20\3\2\2\2\u008b"+
		"\u008c\7c\2\2\u008c\u008d\7w\2\2\u008d\u008e\7v\2\2\u008e\u008f\7q\2\2"+
		"\u008f\u0090\7e\2\2\u0090\u0091\7t\2\2\u0091\u0092\7g\2\2\u0092\u0093"+
		"\7c\2\2\u0093\u0094\7v\2\2\u0094\u00a0\7g\2\2\u0095\u0096\7C\2\2\u0096"+
		"\u0097\7W\2\2\u0097\u0098\7V\2\2\u0098\u0099\7Q\2\2\u0099\u009a\7E\2\2"+
		"\u009a\u009b\7T\2\2\u009b\u009c\7G\2\2\u009c\u009d\7C\2\2\u009d\u009e"+
		"\7V\2\2\u009e\u00a0\7G\2\2\u009f\u008b\3\2\2\2\u009f\u0095\3\2\2\2\u00a0"+
		"\22\3\2\2\2\u00a1\u00a2\7c\2\2\u00a2\u00a3\7w\2\2\u00a3\u00a4\7v\2\2\u00a4"+
		"\u00a5\7q\2\2\u00a5\u00a6\7g\2\2\u00a6\u00a7\7x\2\2\u00a7\u00a8\7q\2\2"+
		"\u00a8\u00a9\7n\2\2\u00a9\u00aa\7x\2\2\u00aa\u00b6\7g\2\2\u00ab\u00ac"+
		"\7C\2\2\u00ac\u00ad\7W\2\2\u00ad\u00ae\7V\2\2\u00ae\u00af\7Q\2\2\u00af"+
		"\u00b0\7G\2\2\u00b0\u00b1\7X\2\2\u00b1\u00b2\7Q\2\2\u00b2\u00b3\7N\2\2"+
		"\u00b3\u00b4\7X\2\2\u00b4\u00b6\7G\2\2\u00b5\u00a1\3\2\2\2\u00b5\u00ab"+
		"\3\2\2\2\u00b6\24\3\2\2\2\u00b7\u00b8\7e\2\2\u00b8\u00b9\7n\2\2\u00b9"+
		"\u00ba\7w\2\2\u00ba\u00bb\7u\2\2\u00bb\u00bc\7v\2\2\u00bc\u00bd\7g\2\2"+
		"\u00bd\u00be\7t\2\2\u00be\u00bf\7d\2\2\u00bf\u00ca\7{\2\2\u00c0\u00c1"+
		"\7E\2\2\u00c1\u00c2\7N\2\2\u00c2\u00c3\7W\2\2\u00c3\u00c4\7U\2\2\u00c4"+
		"\u00c5\7V\2\2\u00c5\u00c6\7G\2\2\u00c6\u00c7\7T\2\2\u00c7\u00c8\7D\2\2"+
		"\u00c8\u00ca\7[\2\2\u00c9\u00b7\3\2\2\2\u00c9\u00c0\3\2\2\2\u00ca\26\3"+
		"\2\2\2\u00cb\u00cc\7d\2\2\u00cc\u00cd\7w\2\2\u00cd\u00ce\7e\2\2\u00ce"+
		"\u00cf\7m\2\2\u00cf\u00d0\7g\2\2\u00d0\u00d1\7v\2\2\u00d1\u00da\7u\2\2"+
		"\u00d2\u00d3\7D\2\2\u00d3\u00d4\7W\2\2\u00d4\u00d5\7E\2\2\u00d5\u00d6"+
		"\7M\2\2\u00d6\u00d7\7G\2\2\u00d7\u00d8\7V\2\2\u00d8\u00da\7U\2\2\u00d9"+
		"\u00cb\3\2\2\2\u00d9\u00d2\3\2\2\2\u00da\30\3\2\2\2\u00db\u00dc\7d\2\2"+
		"\u00dc\u00dd\7c\2\2\u00dd\u00de\7v\2\2\u00de\u00df\7e\2\2\u00df\u00e6"+
		"\7j\2\2\u00e0\u00e1\7D\2\2\u00e1\u00e2\7C\2\2\u00e2\u00e3\7V\2\2\u00e3"+
		"\u00e4\7E\2\2\u00e4\u00e6\7J\2\2\u00e5\u00db\3\2\2\2\u00e5\u00e0\3\2\2"+
		"\2\u00e6\32\3\2\2\2\u00e7\u00e8\7e\2\2\u00e8\u00e9\7c\2\2\u00e9\u00ea"+
		"\7r\2\2\u00ea\u00eb\7k\2\2\u00eb\u00ec\7v\2\2\u00ec\u00ed\7c\2\2\u00ed"+
		"\u00ee\7n\2\2\u00ee\u00ef\7k\2\2\u00ef\u00f0\7|\2\2\u00f0\u00fc\7g\2\2"+
		"\u00f1\u00f2\7E\2\2\u00f2\u00f3\7C\2\2\u00f3\u00f4\7R\2\2\u00f4\u00f5"+
		"\7K\2\2\u00f5\u00f6\7V\2\2\u00f6\u00f7\7C\2\2\u00f7\u00f8\7N\2\2\u00f8"+
		"\u00f9\7K\2\2\u00f9\u00fa\7\\\2\2\u00fa\u00fc\7G\2\2\u00fb\u00e7\3\2\2"+
		"\2\u00fb\u00f1\3\2\2\2\u00fc\34\3\2\2\2\u00fd\u00fe\7r\2\2\u00fe\u00ff"+
		"\7c\2\2\u00ff\u0100\7t\2\2\u0100\u0101\7v\2\2\u0101\u0102\7k\2\2\u0102"+
		"\u0103\7v\2\2\u0103\u0104\7k\2\2\u0104\u0105\7q\2\2\u0105\u0106\7p\2\2"+
		"\u0106\u0107\7d\2\2\u0107\u0114\7{\2\2\u0108\u0109\7R\2\2\u0109\u010a"+
		"\7C\2\2\u010a\u010b\7T\2\2\u010b\u010c\7V\2\2\u010c\u010d\7K\2\2\u010d"+
		"\u010e\7V\2\2\u010e\u010f\7K\2\2\u010f\u0110\7Q\2\2\u0110\u0111\7P\2\2"+
		"\u0111\u0112\7D\2\2\u0112\u0114\7[\2\2\u0113\u00fd\3\2\2\2\u0113\u0108"+
		"\3\2\2\2\u0114\36\3\2\2\2\u0115\u0116\7f\2\2\u0116\u0117\7k\2\2\u0117"+
		"\u0118\7u\2\2\u0118\u0119\7v\2\2\u0119\u011a\7t\2\2\u011a\u011b\7k\2\2"+
		"\u011b\u011c\7d\2\2\u011c\u011d\7w\2\2\u011d\u011e\7v\2\2\u011e\u011f"+
		"\7g\2\2\u011f\u0120\7d\2\2\u0120\u012e\7{\2\2\u0121\u0122\7F\2\2\u0122"+
		"\u0123\7K\2\2\u0123\u0124\7U\2\2\u0124\u0125\7V\2\2\u0125\u0126\7T\2\2"+
		"\u0126\u0127\7K\2\2\u0127\u0128\7D\2\2\u0128\u0129\7W\2\2\u0129\u012a"+
		"\7V\2\2\u012a\u012b\7G\2\2\u012b\u012c\7D\2\2\u012c\u012e\7[\2\2\u012d"+
		"\u0115\3\2\2\2\u012d\u0121\3\2\2\2\u012e \3\2\2\2\u012f\u0130\7y\2\2\u0130"+
		"\u0131\7k\2\2\u0131\u0132\7v\2\2\u0132\u0133\7j\2\2\u0133\u0134\7v\2\2"+
		"\u0134\u0135\7k\2\2\u0135\u0136\7o\2\2\u0136\u0137\7g\2\2\u0137\u0138"+
		"\7u\2\2\u0138\u0139\7v\2\2\u0139\u013a\7c\2\2\u013a\u013b\7o\2\2\u013b"+
		"\u014a\7r\2\2\u013c\u013d\7Y\2\2\u013d\u013e\7K\2\2\u013e\u013f\7V\2\2"+
		"\u013f\u0140\7J\2\2\u0140\u0141\7V\2\2\u0141\u0142\7K\2\2\u0142\u0143"+
		"\7O\2\2\u0143\u0144\7G\2\2\u0144\u0145\7U\2\2\u0145\u0146\7V\2\2\u0146"+
		"\u0147\7C\2\2\u0147\u0148\7O\2\2\u0148\u014a\7R\2\2\u0149\u012f\3\2\2"+
		"\2\u0149\u013c\3\2\2\2\u014a\"\3\2\2\2\u014b\u014c\7u\2\2\u014c\u014d"+
		"\7{\2\2\u014d\u014e\7u\2\2\u014e\u014f\7a\2\2\u014f\u0150\7v\2\2\u0150"+
		"\u0151\7k\2\2\u0151\u0152\7o\2\2\u0152\u0153\7g\2\2\u0153\u0154\7*\2\2"+
		"\u0154\u0160\7+\2\2\u0155\u0156\7U\2\2\u0156\u0157\7[\2\2\u0157\u0158"+
		"\7U\2\2\u0158\u0159\7a\2\2\u0159\u015a\7V\2\2\u015a\u015b\7K\2\2\u015b"+
		"\u015c\7O\2\2\u015c\u015d\7G\2\2\u015d\u015e\7*\2\2\u015e\u0160\7+\2\2"+
		"\u015f\u014b\3\2\2\2\u015f\u0155\3\2\2\2\u0160$\3\2\2\2\u0161\u0162\7"+
		"r\2\2\u0162\u0166\7m\2\2\u0163\u0164\7R\2\2\u0164\u0166\7M\2\2\u0165\u0161"+
		"\3\2\2\2\u0165\u0163\3\2\2\2\u0166&\3\2\2\2\u0167\u0169\4\62;\2\u0168"+
		"\u0167\3\2\2\2\u0169\u016a\3\2\2\2\u016a\u0168\3\2\2\2\u016a\u016b\3\2"+
		"\2\2\u016b(\3\2\2\2\u016c\u016d\7,\2\2\u016d*\3\2\2\2\u016e\u016f\7.\2"+
		"\2\u016f,\3\2\2\2\u0170\u0171\7\60\2\2\u0171.\3\2\2\2\u0172\u0174\t\2"+
		"\2\2\u0173\u0172\3\2\2\2\u0174\u0175\3\2\2\2\u0175\u0173\3\2\2\2\u0175"+
		"\u0176\3\2\2\2\u0176\60\3\2\2\2\u0177\u0179\t\3\2\2\u0178\u0177\3\2\2"+
		"\2\u0179\u017a\3\2\2\2\u017a\u0178\3\2\2\2\u017a\u017b\3\2\2\2\u017b\62"+
		"\3\2\2\2\u017c\u017e\7\17\2\2\u017d\u017c\3\2\2\2\u017d\u017e\3\2\2\2"+
		"\u017e\u017f\3\2\2\2\u017f\u0180\7\f\2\2\u0180\u0181\3\2\2\2\u0181\u0182"+
		"\b\32\2\2\u0182\64\3\2\2\2\u0183\u0185\t\4\2\2\u0184\u0183\3\2\2\2\u0185"+
		"\u0186\3\2\2\2\u0186\u0184\3\2\2\2\u0186\u0187\3\2\2\2\u0187\u0188\3\2"+
		"\2\2\u0188\u0189\b\33\2\2\u0189\66\3\2\2\2\u018a\u018b\7?\2\2\u018b8\3"+
		"\2\2\2\32\2ES]ku\u0083\u0089\u009f\u00b5\u00c9\u00d9\u00e5\u00fb\u0113"+
		"\u012d\u0149\u015f\u0165\u016a\u0175\u017a\u017d\u0186\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}