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
		PK=15, INT=16, ASTERISK=17, COMMA=18, DOT=19, ID=20, TOPICNAME=21, NEWLINE=22, 
		WS=23, EQUAL=24;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "PARTITIONBY", 
		"PK", "INT", "ASTERISK", "COMMA", "DOT", "ID", "TOPICNAME", "NEWLINE", 
		"WS", "EQUAL"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, "'*'", "','", "'.'", null, null, null, null, 
		"'='"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "PARTITIONBY", 
		"PK", "INT", "ASTERISK", "COMMA", "DOT", "ID", "TOPICNAME", "NEWLINE", 
		"WS", "EQUAL"
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\32\u0142\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\5\2@\n\2\3\3\3\3"+
		"\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3N\n\3\3\4\3\4\3\4\3\4\3\4"+
		"\3\4\3\4\3\4\5\4X\n\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5"+
		"\5\5f\n\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6p\n\6\3\7\3\7\3\7\3\7\3\7"+
		"\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7~\n\7\3\b\3\b\3\b\3\b\5\b\u0084\n\b\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\5\t\u009a\n\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u00b0\n\n\3\13\3\13\3\13\3\13\3\13"+
		"\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\5\13"+
		"\u00c4\n\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\5"+
		"\f\u00d4\n\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\5\r\u00e0\n\r\3\16"+
		"\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16"+
		"\3\16\3\16\3\16\3\16\3\16\5\16\u00f6\n\16\3\17\3\17\3\17\3\17\3\17\3\17"+
		"\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
		"\3\17\3\17\5\17\u010e\n\17\3\20\3\20\3\20\3\20\5\20\u0114\n\20\3\21\6"+
		"\21\u0117\n\21\r\21\16\21\u0118\3\22\3\22\3\23\3\23\3\24\3\24\3\25\6\25"+
		"\u0122\n\25\r\25\16\25\u0123\3\26\3\26\5\26\u0128\n\26\3\26\5\26\u012b"+
		"\n\26\3\26\5\26\u012e\n\26\3\26\5\26\u0131\n\26\3\27\5\27\u0134\n\27\3"+
		"\27\3\27\3\27\3\27\3\30\6\30\u013b\n\30\r\30\16\30\u013c\3\30\3\30\3\31"+
		"\3\31\2\2\32\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16"+
		"\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\3\2\4\6\2\62"+
		";C\\aac|\5\2\13\f\17\17\"\"\u0158\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2"+
		"\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3"+
		"\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2"+
		"\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2"+
		"\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\3?\3\2\2\2\5M\3\2\2\2"+
		"\7W\3\2\2\2\te\3\2\2\2\13o\3\2\2\2\r}\3\2\2\2\17\u0083\3\2\2\2\21\u0099"+
		"\3\2\2\2\23\u00af\3\2\2\2\25\u00c3\3\2\2\2\27\u00d3\3\2\2\2\31\u00df\3"+
		"\2\2\2\33\u00f5\3\2\2\2\35\u010d\3\2\2\2\37\u0113\3\2\2\2!\u0116\3\2\2"+
		"\2#\u011a\3\2\2\2%\u011c\3\2\2\2\'\u011e\3\2\2\2)\u0121\3\2\2\2+\u0125"+
		"\3\2\2\2-\u0133\3\2\2\2/\u013a\3\2\2\2\61\u0140\3\2\2\2\63\64\7k\2\2\64"+
		"\65\7p\2\2\65\66\7u\2\2\66\67\7g\2\2\678\7t\2\28@\7v\2\29:\7K\2\2:;\7"+
		"P\2\2;<\7U\2\2<=\7G\2\2=>\7T\2\2>@\7V\2\2?\63\3\2\2\2?9\3\2\2\2@\4\3\2"+
		"\2\2AB\7w\2\2BC\7r\2\2CD\7u\2\2DE\7g\2\2EF\7t\2\2FN\7v\2\2GH\7W\2\2HI"+
		"\7R\2\2IJ\7U\2\2JK\7G\2\2KL\7T\2\2LN\7V\2\2MA\3\2\2\2MG\3\2\2\2N\6\3\2"+
		"\2\2OP\7k\2\2PQ\7p\2\2QR\7v\2\2RX\7q\2\2ST\7K\2\2TU\7P\2\2UV\7V\2\2VX"+
		"\7Q\2\2WO\3\2\2\2WS\3\2\2\2X\b\3\2\2\2YZ\7u\2\2Z[\7g\2\2[\\\7n\2\2\\]"+
		"\7g\2\2]^\7e\2\2^f\7v\2\2_`\7U\2\2`a\7G\2\2ab\7N\2\2bc\7G\2\2cd\7E\2\2"+
		"df\7V\2\2eY\3\2\2\2e_\3\2\2\2f\n\3\2\2\2gh\7h\2\2hi\7t\2\2ij\7q\2\2jp"+
		"\7o\2\2kl\7H\2\2lm\7T\2\2mn\7Q\2\2np\7O\2\2og\3\2\2\2ok\3\2\2\2p\f\3\2"+
		"\2\2qr\7k\2\2rs\7i\2\2st\7p\2\2tu\7q\2\2uv\7t\2\2v~\7g\2\2wx\7K\2\2xy"+
		"\7I\2\2yz\7P\2\2z{\7Q\2\2{|\7T\2\2|~\7G\2\2}q\3\2\2\2}w\3\2\2\2~\16\3"+
		"\2\2\2\177\u0080\7c\2\2\u0080\u0084\7u\2\2\u0081\u0082\7C\2\2\u0082\u0084"+
		"\7U\2\2\u0083\177\3\2\2\2\u0083\u0081\3\2\2\2\u0084\20\3\2\2\2\u0085\u0086"+
		"\7c\2\2\u0086\u0087\7w\2\2\u0087\u0088\7v\2\2\u0088\u0089\7q\2\2\u0089"+
		"\u008a\7e\2\2\u008a\u008b\7t\2\2\u008b\u008c\7g\2\2\u008c\u008d\7c\2\2"+
		"\u008d\u008e\7v\2\2\u008e\u009a\7g\2\2\u008f\u0090\7C\2\2\u0090\u0091"+
		"\7W\2\2\u0091\u0092\7V\2\2\u0092\u0093\7Q\2\2\u0093\u0094\7E\2\2\u0094"+
		"\u0095\7T\2\2\u0095\u0096\7G\2\2\u0096\u0097\7C\2\2\u0097\u0098\7V\2\2"+
		"\u0098\u009a\7G\2\2\u0099\u0085\3\2\2\2\u0099\u008f\3\2\2\2\u009a\22\3"+
		"\2\2\2\u009b\u009c\7c\2\2\u009c\u009d\7w\2\2\u009d\u009e\7v\2\2\u009e"+
		"\u009f\7q\2\2\u009f\u00a0\7g\2\2\u00a0\u00a1\7x\2\2\u00a1\u00a2\7q\2\2"+
		"\u00a2\u00a3\7n\2\2\u00a3\u00a4\7x\2\2\u00a4\u00b0\7g\2\2\u00a5\u00a6"+
		"\7C\2\2\u00a6\u00a7\7W\2\2\u00a7\u00a8\7V\2\2\u00a8\u00a9\7Q\2\2\u00a9"+
		"\u00aa\7G\2\2\u00aa\u00ab\7X\2\2\u00ab\u00ac\7Q\2\2\u00ac\u00ad\7N\2\2"+
		"\u00ad\u00ae\7X\2\2\u00ae\u00b0\7G\2\2\u00af\u009b\3\2\2\2\u00af\u00a5"+
		"\3\2\2\2\u00b0\24\3\2\2\2\u00b1\u00b2\7e\2\2\u00b2\u00b3\7n\2\2\u00b3"+
		"\u00b4\7w\2\2\u00b4\u00b5\7u\2\2\u00b5\u00b6\7v\2\2\u00b6\u00b7\7g\2\2"+
		"\u00b7\u00b8\7t\2\2\u00b8\u00b9\7d\2\2\u00b9\u00c4\7{\2\2\u00ba\u00bb"+
		"\7E\2\2\u00bb\u00bc\7N\2\2\u00bc\u00bd\7W\2\2\u00bd\u00be\7U\2\2\u00be"+
		"\u00bf\7V\2\2\u00bf\u00c0\7G\2\2\u00c0\u00c1\7T\2\2\u00c1\u00c2\7D\2\2"+
		"\u00c2\u00c4\7[\2\2\u00c3\u00b1\3\2\2\2\u00c3\u00ba\3\2\2\2\u00c4\26\3"+
		"\2\2\2\u00c5\u00c6\7d\2\2\u00c6\u00c7\7w\2\2\u00c7\u00c8\7e\2\2\u00c8"+
		"\u00c9\7m\2\2\u00c9\u00ca\7g\2\2\u00ca\u00cb\7v\2\2\u00cb\u00d4\7u\2\2"+
		"\u00cc\u00cd\7D\2\2\u00cd\u00ce\7W\2\2\u00ce\u00cf\7E\2\2\u00cf\u00d0"+
		"\7M\2\2\u00d0\u00d1\7G\2\2\u00d1\u00d2\7V\2\2\u00d2\u00d4\7U\2\2\u00d3"+
		"\u00c5\3\2\2\2\u00d3\u00cc\3\2\2\2\u00d4\30\3\2\2\2\u00d5\u00d6\7d\2\2"+
		"\u00d6\u00d7\7c\2\2\u00d7\u00d8\7v\2\2\u00d8\u00d9\7e\2\2\u00d9\u00e0"+
		"\7j\2\2\u00da\u00db\7D\2\2\u00db\u00dc\7C\2\2\u00dc\u00dd\7V\2\2\u00dd"+
		"\u00de\7E\2\2\u00de\u00e0\7J\2\2\u00df\u00d5\3\2\2\2\u00df\u00da\3\2\2"+
		"\2\u00e0\32\3\2\2\2\u00e1\u00e2\7e\2\2\u00e2\u00e3\7c\2\2\u00e3\u00e4"+
		"\7r\2\2\u00e4\u00e5\7k\2\2\u00e5\u00e6\7v\2\2\u00e6\u00e7\7c\2\2\u00e7"+
		"\u00e8\7n\2\2\u00e8\u00e9\7k\2\2\u00e9\u00ea\7|\2\2\u00ea\u00f6\7g\2\2"+
		"\u00eb\u00ec\7E\2\2\u00ec\u00ed\7C\2\2\u00ed\u00ee\7R\2\2\u00ee\u00ef"+
		"\7K\2\2\u00ef\u00f0\7V\2\2\u00f0\u00f1\7C\2\2\u00f1\u00f2\7N\2\2\u00f2"+
		"\u00f3\7K\2\2\u00f3\u00f4\7\\\2\2\u00f4\u00f6\7G\2\2\u00f5\u00e1\3\2\2"+
		"\2\u00f5\u00eb\3\2\2\2\u00f6\34\3\2\2\2\u00f7\u00f8\7r\2\2\u00f8\u00f9"+
		"\7c\2\2\u00f9\u00fa\7t\2\2\u00fa\u00fb\7v\2\2\u00fb\u00fc\7k\2\2\u00fc"+
		"\u00fd\7v\2\2\u00fd\u00fe\7k\2\2\u00fe\u00ff\7q\2\2\u00ff\u0100\7p\2\2"+
		"\u0100\u0101\7d\2\2\u0101\u010e\7{\2\2\u0102\u0103\7R\2\2\u0103\u0104"+
		"\7C\2\2\u0104\u0105\7T\2\2\u0105\u0106\7V\2\2\u0106\u0107\7K\2\2\u0107"+
		"\u0108\7V\2\2\u0108\u0109\7K\2\2\u0109\u010a\7Q\2\2\u010a\u010b\7P\2\2"+
		"\u010b\u010c\7D\2\2\u010c\u010e\7[\2\2\u010d\u00f7\3\2\2\2\u010d\u0102"+
		"\3\2\2\2\u010e\36\3\2\2\2\u010f\u0110\7r\2\2\u0110\u0114\7m\2\2\u0111"+
		"\u0112\7R\2\2\u0112\u0114\7M\2\2\u0113\u010f\3\2\2\2\u0113\u0111\3\2\2"+
		"\2\u0114 \3\2\2\2\u0115\u0117\4\62;\2\u0116\u0115\3\2\2\2\u0117\u0118"+
		"\3\2\2\2\u0118\u0116\3\2\2\2\u0118\u0119\3\2\2\2\u0119\"\3\2\2\2\u011a"+
		"\u011b\7,\2\2\u011b$\3\2\2\2\u011c\u011d\7.\2\2\u011d&\3\2\2\2\u011e\u011f"+
		"\7\60\2\2\u011f(\3\2\2\2\u0120\u0122\t\2\2\2\u0121\u0120\3\2\2\2\u0122"+
		"\u0123\3\2\2\2\u0123\u0121\3\2\2\2\u0123\u0124\3\2\2\2\u0124*\3\2\2\2"+
		"\u0125\u0130\5)\25\2\u0126\u0128\7\60\2\2\u0127\u0126\3\2\2\2\u0127\u0128"+
		"\3\2\2\2\u0128\u012a\3\2\2\2\u0129\u012b\7/\2\2\u012a\u0129\3\2\2\2\u012a"+
		"\u012b\3\2\2\2\u012b\u012d\3\2\2\2\u012c\u012e\7-\2\2\u012d\u012c\3\2"+
		"\2\2\u012d\u012e\3\2\2\2\u012e\u012f\3\2\2\2\u012f\u0131\5)\25\2\u0130"+
		"\u0127\3\2\2\2\u0130\u0131\3\2\2\2\u0131,\3\2\2\2\u0132\u0134\7\17\2\2"+
		"\u0133\u0132\3\2\2\2\u0133\u0134\3\2\2\2\u0134\u0135\3\2\2\2\u0135\u0136"+
		"\7\f\2\2\u0136\u0137\3\2\2\2\u0137\u0138\b\27\2\2\u0138.\3\2\2\2\u0139"+
		"\u013b\t\3\2\2\u013a\u0139\3\2\2\2\u013b\u013c\3\2\2\2\u013c\u013a\3\2"+
		"\2\2\u013c\u013d\3\2\2\2\u013d\u013e\3\2\2\2\u013e\u013f\b\30\2\2\u013f"+
		"\60\3\2\2\2\u0140\u0141\7?\2\2\u0141\62\3\2\2\2\32\2?MWeo}\u0083\u0099"+
		"\u00af\u00c3\u00d3\u00df\u00f5\u010d\u0113\u0118\u0123\u0127\u012a\u012d"+
		"\u0130\u0133\u013c\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}