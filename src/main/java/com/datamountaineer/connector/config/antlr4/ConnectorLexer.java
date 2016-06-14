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
		AUTOEVOLVE=9, BATCH=10, CAPITALIZE=11, PARTITIONBY=12, PK=13, INT=14, 
		ASTERISK=15, COMMA=16, DOT=17, ID=18, TOPICNAME=19, NEWLINE=20, WS=21, 
		EQUAL=22;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "BATCH", "CAPITALIZE", "PARTITIONBY", "PK", "INT", "ASTERISK", 
		"COMMA", "DOT", "ID", "TOPICNAME", "NEWLINE", "WS", "EQUAL"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, "'*'", "','", "'.'", null, null, null, null, "'='"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "BATCH", "CAPITALIZE", "PARTITIONBY", "PK", "INT", "ASTERISK", 
		"COMMA", "DOT", "ID", "TOPICNAME", "NEWLINE", "WS", "EQUAL"
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\30\u011a\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\3\2\3\2\3\2\3"+
		"\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\5\2<\n\2\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\3\3\3\3\3\5\3J\n\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4T\n"+
		"\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5b\n\5\3\6\3\6\3"+
		"\6\3\6\3\6\3\6\3\6\3\6\5\6l\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3"+
		"\7\3\7\3\7\5\7z\n\7\3\b\3\b\3\b\3\b\5\b\u0080\n\b\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0096"+
		"\n\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\n\3\n\5\n\u00ac\n\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3"+
		"\13\3\13\5\13\u00b8\n\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3"+
		"\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u00ce\n\f\3\r\3\r\3\r\3\r\3\r\3"+
		"\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\5\r"+
		"\u00e6\n\r\3\16\3\16\3\16\3\16\5\16\u00ec\n\16\3\17\6\17\u00ef\n\17\r"+
		"\17\16\17\u00f0\3\20\3\20\3\21\3\21\3\22\3\22\3\23\6\23\u00fa\n\23\r\23"+
		"\16\23\u00fb\3\24\3\24\5\24\u0100\n\24\3\24\5\24\u0103\n\24\3\24\5\24"+
		"\u0106\n\24\3\24\5\24\u0109\n\24\3\25\5\25\u010c\n\25\3\25\3\25\3\25\3"+
		"\25\3\26\6\26\u0113\n\26\r\26\16\26\u0114\3\26\3\26\3\27\3\27\2\2\30\3"+
		"\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37"+
		"\21!\22#\23%\24\'\25)\26+\27-\30\3\2\4\6\2\62;C\\aac|\5\2\13\f\17\17\""+
		"\"\u012e\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2"+
		"\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27"+
		"\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2"+
		"\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2"+
		"\3;\3\2\2\2\5I\3\2\2\2\7S\3\2\2\2\ta\3\2\2\2\13k\3\2\2\2\ry\3\2\2\2\17"+
		"\177\3\2\2\2\21\u0095\3\2\2\2\23\u00ab\3\2\2\2\25\u00b7\3\2\2\2\27\u00cd"+
		"\3\2\2\2\31\u00e5\3\2\2\2\33\u00eb\3\2\2\2\35\u00ee\3\2\2\2\37\u00f2\3"+
		"\2\2\2!\u00f4\3\2\2\2#\u00f6\3\2\2\2%\u00f9\3\2\2\2\'\u00fd\3\2\2\2)\u010b"+
		"\3\2\2\2+\u0112\3\2\2\2-\u0118\3\2\2\2/\60\7k\2\2\60\61\7p\2\2\61\62\7"+
		"u\2\2\62\63\7g\2\2\63\64\7t\2\2\64<\7v\2\2\65\66\7K\2\2\66\67\7P\2\2\67"+
		"8\7U\2\289\7G\2\29:\7T\2\2:<\7V\2\2;/\3\2\2\2;\65\3\2\2\2<\4\3\2\2\2="+
		">\7w\2\2>?\7r\2\2?@\7u\2\2@A\7g\2\2AB\7t\2\2BJ\7v\2\2CD\7W\2\2DE\7R\2"+
		"\2EF\7U\2\2FG\7G\2\2GH\7T\2\2HJ\7V\2\2I=\3\2\2\2IC\3\2\2\2J\6\3\2\2\2"+
		"KL\7k\2\2LM\7p\2\2MN\7v\2\2NT\7q\2\2OP\7K\2\2PQ\7P\2\2QR\7V\2\2RT\7Q\2"+
		"\2SK\3\2\2\2SO\3\2\2\2T\b\3\2\2\2UV\7u\2\2VW\7g\2\2WX\7n\2\2XY\7g\2\2"+
		"YZ\7e\2\2Zb\7v\2\2[\\\7U\2\2\\]\7G\2\2]^\7N\2\2^_\7G\2\2_`\7E\2\2`b\7"+
		"V\2\2aU\3\2\2\2a[\3\2\2\2b\n\3\2\2\2cd\7h\2\2de\7t\2\2ef\7q\2\2fl\7o\2"+
		"\2gh\7H\2\2hi\7T\2\2ij\7Q\2\2jl\7O\2\2kc\3\2\2\2kg\3\2\2\2l\f\3\2\2\2"+
		"mn\7k\2\2no\7i\2\2op\7p\2\2pq\7q\2\2qr\7t\2\2rz\7g\2\2st\7K\2\2tu\7I\2"+
		"\2uv\7P\2\2vw\7Q\2\2wx\7T\2\2xz\7G\2\2ym\3\2\2\2ys\3\2\2\2z\16\3\2\2\2"+
		"{|\7c\2\2|\u0080\7u\2\2}~\7C\2\2~\u0080\7U\2\2\177{\3\2\2\2\177}\3\2\2"+
		"\2\u0080\20\3\2\2\2\u0081\u0082\7c\2\2\u0082\u0083\7w\2\2\u0083\u0084"+
		"\7v\2\2\u0084\u0085\7q\2\2\u0085\u0086\7e\2\2\u0086\u0087\7t\2\2\u0087"+
		"\u0088\7g\2\2\u0088\u0089\7c\2\2\u0089\u008a\7v\2\2\u008a\u0096\7g\2\2"+
		"\u008b\u008c\7C\2\2\u008c\u008d\7W\2\2\u008d\u008e\7V\2\2\u008e\u008f"+
		"\7Q\2\2\u008f\u0090\7E\2\2\u0090\u0091\7T\2\2\u0091\u0092\7G\2\2\u0092"+
		"\u0093\7C\2\2\u0093\u0094\7V\2\2\u0094\u0096\7G\2\2\u0095\u0081\3\2\2"+
		"\2\u0095\u008b\3\2\2\2\u0096\22\3\2\2\2\u0097\u0098\7c\2\2\u0098\u0099"+
		"\7w\2\2\u0099\u009a\7v\2\2\u009a\u009b\7q\2\2\u009b\u009c\7g\2\2\u009c"+
		"\u009d\7x\2\2\u009d\u009e\7q\2\2\u009e\u009f\7n\2\2\u009f\u00a0\7x\2\2"+
		"\u00a0\u00ac\7g\2\2\u00a1\u00a2\7C\2\2\u00a2\u00a3\7W\2\2\u00a3\u00a4"+
		"\7V\2\2\u00a4\u00a5\7Q\2\2\u00a5\u00a6\7G\2\2\u00a6\u00a7\7X\2\2\u00a7"+
		"\u00a8\7Q\2\2\u00a8\u00a9\7N\2\2\u00a9\u00aa\7X\2\2\u00aa\u00ac\7G\2\2"+
		"\u00ab\u0097\3\2\2\2\u00ab\u00a1\3\2\2\2\u00ac\24\3\2\2\2\u00ad\u00ae"+
		"\7d\2\2\u00ae\u00af\7c\2\2\u00af\u00b0\7v\2\2\u00b0\u00b1\7e\2\2\u00b1"+
		"\u00b8\7j\2\2\u00b2\u00b3\7D\2\2\u00b3\u00b4\7C\2\2\u00b4\u00b5\7V\2\2"+
		"\u00b5\u00b6\7E\2\2\u00b6\u00b8\7J\2\2\u00b7\u00ad\3\2\2\2\u00b7\u00b2"+
		"\3\2\2\2\u00b8\26\3\2\2\2\u00b9\u00ba\7e\2\2\u00ba\u00bb\7c\2\2\u00bb"+
		"\u00bc\7r\2\2\u00bc\u00bd\7k\2\2\u00bd\u00be\7v\2\2\u00be\u00bf\7c\2\2"+
		"\u00bf\u00c0\7n\2\2\u00c0\u00c1\7k\2\2\u00c1\u00c2\7|\2\2\u00c2\u00ce"+
		"\7g\2\2\u00c3\u00c4\7E\2\2\u00c4\u00c5\7C\2\2\u00c5\u00c6\7R\2\2\u00c6"+
		"\u00c7\7K\2\2\u00c7\u00c8\7V\2\2\u00c8\u00c9\7C\2\2\u00c9\u00ca\7N\2\2"+
		"\u00ca\u00cb\7K\2\2\u00cb\u00cc\7\\\2\2\u00cc\u00ce\7G\2\2\u00cd\u00b9"+
		"\3\2\2\2\u00cd\u00c3\3\2\2\2\u00ce\30\3\2\2\2\u00cf\u00d0\7r\2\2\u00d0"+
		"\u00d1\7c\2\2\u00d1\u00d2\7t\2\2\u00d2\u00d3\7v\2\2\u00d3\u00d4\7k\2\2"+
		"\u00d4\u00d5\7v\2\2\u00d5\u00d6\7k\2\2\u00d6\u00d7\7q\2\2\u00d7\u00d8"+
		"\7p\2\2\u00d8\u00d9\7d\2\2\u00d9\u00e6\7{\2\2\u00da\u00db\7R\2\2\u00db"+
		"\u00dc\7C\2\2\u00dc\u00dd\7T\2\2\u00dd\u00de\7V\2\2\u00de\u00df\7K\2\2"+
		"\u00df\u00e0\7V\2\2\u00e0\u00e1\7K\2\2\u00e1\u00e2\7Q\2\2\u00e2\u00e3"+
		"\7P\2\2\u00e3\u00e4\7D\2\2\u00e4\u00e6\7[\2\2\u00e5\u00cf\3\2\2\2\u00e5"+
		"\u00da\3\2\2\2\u00e6\32\3\2\2\2\u00e7\u00e8\7r\2\2\u00e8\u00ec\7m\2\2"+
		"\u00e9\u00ea\7R\2\2\u00ea\u00ec\7M\2\2\u00eb\u00e7\3\2\2\2\u00eb\u00e9"+
		"\3\2\2\2\u00ec\34\3\2\2\2\u00ed\u00ef\4\62;\2\u00ee\u00ed\3\2\2\2\u00ef"+
		"\u00f0\3\2\2\2\u00f0\u00ee\3\2\2\2\u00f0\u00f1\3\2\2\2\u00f1\36\3\2\2"+
		"\2\u00f2\u00f3\7,\2\2\u00f3 \3\2\2\2\u00f4\u00f5\7.\2\2\u00f5\"\3\2\2"+
		"\2\u00f6\u00f7\7\60\2\2\u00f7$\3\2\2\2\u00f8\u00fa\t\2\2\2\u00f9\u00f8"+
		"\3\2\2\2\u00fa\u00fb\3\2\2\2\u00fb\u00f9\3\2\2\2\u00fb\u00fc\3\2\2\2\u00fc"+
		"&\3\2\2\2\u00fd\u0108\5%\23\2\u00fe\u0100\7\60\2\2\u00ff\u00fe\3\2\2\2"+
		"\u00ff\u0100\3\2\2\2\u0100\u0102\3\2\2\2\u0101\u0103\7/\2\2\u0102\u0101"+
		"\3\2\2\2\u0102\u0103\3\2\2\2\u0103\u0105\3\2\2\2\u0104\u0106\7-\2\2\u0105"+
		"\u0104\3\2\2\2\u0105\u0106\3\2\2\2\u0106\u0107\3\2\2\2\u0107\u0109\5%"+
		"\23\2\u0108\u00ff\3\2\2\2\u0108\u0109\3\2\2\2\u0109(\3\2\2\2\u010a\u010c"+
		"\7\17\2\2\u010b\u010a\3\2\2\2\u010b\u010c\3\2\2\2\u010c\u010d\3\2\2\2"+
		"\u010d\u010e\7\f\2\2\u010e\u010f\3\2\2\2\u010f\u0110\b\25\2\2\u0110*\3"+
		"\2\2\2\u0111\u0113\t\3\2\2\u0112\u0111\3\2\2\2\u0113\u0114\3\2\2\2\u0114"+
		"\u0112\3\2\2\2\u0114\u0115\3\2\2\2\u0115\u0116\3\2\2\2\u0116\u0117\b\26"+
		"\2\2\u0117,\3\2\2\2\u0118\u0119\7?\2\2\u0119.\3\2\2\2\30\2;ISaky\177\u0095"+
		"\u00ab\u00b7\u00cd\u00e5\u00eb\u00f0\u00fb\u00ff\u0102\u0105\u0108\u010b"+
		"\u0114\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}