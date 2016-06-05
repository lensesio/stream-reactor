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
		AUTOEVOLVE=9, BATCH=10, CAPITALIZE=11, PK=12, INT=13, ASTERISK=14, COMMA=15, 
		DOT=16, ID=17, TOPICNAME=18, NEWLINE=19, WS=20, EQUAL=21;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "BATCH", "CAPITALIZE", "PK", "INT", "ASTERISK", "COMMA", 
		"DOT", "ID", "TOPICNAME", "NEWLINE", "WS", "EQUAL"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, "'*'", "','", "'.'", null, null, null, null, "'='"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "BATCH", "CAPITALIZE", "PK", "INT", "ASTERISK", "COMMA", 
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\27\u00f8\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\3\2\3\2\3\2\3\2\3\2\3\2"+
		"\3\2\3\2\3\2\3\2\3\2\3\2\5\2:\n\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\5\3H\n\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4R\n\4\3\5\3\5"+
		"\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5`\n\5\3\6\3\6\3\6\3\6\3\6"+
		"\3\6\3\6\3\6\5\6j\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7"+
		"\5\7x\n\7\3\b\3\b\3\b\3\b\5\b~\n\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"+
		"\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0094\n\t\3\n\3\n\3\n"+
		"\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\5"+
		"\n\u00aa\n\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\5\13\u00b6"+
		"\n\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f"+
		"\3\f\3\f\3\f\3\f\5\f\u00cc\n\f\3\r\3\r\3\r\3\r\5\r\u00d2\n\r\3\16\6\16"+
		"\u00d5\n\16\r\16\16\16\u00d6\3\17\3\17\3\20\3\20\3\21\3\21\3\22\6\22\u00e0"+
		"\n\22\r\22\16\22\u00e1\3\23\6\23\u00e5\n\23\r\23\16\23\u00e6\3\24\5\24"+
		"\u00ea\n\24\3\24\3\24\3\24\3\24\3\25\6\25\u00f1\n\25\r\25\16\25\u00f2"+
		"\3\25\3\25\3\26\3\26\2\2\27\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25"+
		"\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'\25)\26+\27\3\2\5\6\2\62"+
		";C\\aac|\b\2--/\60\62;C\\aac|\5\2\13\f\17\17\"\"\u0108\2\3\3\2\2\2\2\5"+
		"\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2"+
		"\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33"+
		"\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2"+
		"\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\39\3\2\2\2\5G\3\2\2\2\7Q\3\2\2\2\t_\3"+
		"\2\2\2\13i\3\2\2\2\rw\3\2\2\2\17}\3\2\2\2\21\u0093\3\2\2\2\23\u00a9\3"+
		"\2\2\2\25\u00b5\3\2\2\2\27\u00cb\3\2\2\2\31\u00d1\3\2\2\2\33\u00d4\3\2"+
		"\2\2\35\u00d8\3\2\2\2\37\u00da\3\2\2\2!\u00dc\3\2\2\2#\u00df\3\2\2\2%"+
		"\u00e4\3\2\2\2\'\u00e9\3\2\2\2)\u00f0\3\2\2\2+\u00f6\3\2\2\2-.\7k\2\2"+
		"./\7p\2\2/\60\7u\2\2\60\61\7g\2\2\61\62\7t\2\2\62:\7v\2\2\63\64\7K\2\2"+
		"\64\65\7P\2\2\65\66\7U\2\2\66\67\7G\2\2\678\7T\2\28:\7V\2\29-\3\2\2\2"+
		"9\63\3\2\2\2:\4\3\2\2\2;<\7w\2\2<=\7r\2\2=>\7u\2\2>?\7g\2\2?@\7t\2\2@"+
		"H\7v\2\2AB\7W\2\2BC\7R\2\2CD\7U\2\2DE\7G\2\2EF\7T\2\2FH\7V\2\2G;\3\2\2"+
		"\2GA\3\2\2\2H\6\3\2\2\2IJ\7k\2\2JK\7p\2\2KL\7v\2\2LR\7q\2\2MN\7K\2\2N"+
		"O\7P\2\2OP\7V\2\2PR\7Q\2\2QI\3\2\2\2QM\3\2\2\2R\b\3\2\2\2ST\7u\2\2TU\7"+
		"g\2\2UV\7n\2\2VW\7g\2\2WX\7e\2\2X`\7v\2\2YZ\7U\2\2Z[\7G\2\2[\\\7N\2\2"+
		"\\]\7G\2\2]^\7E\2\2^`\7V\2\2_S\3\2\2\2_Y\3\2\2\2`\n\3\2\2\2ab\7h\2\2b"+
		"c\7t\2\2cd\7q\2\2dj\7o\2\2ef\7H\2\2fg\7T\2\2gh\7Q\2\2hj\7O\2\2ia\3\2\2"+
		"\2ie\3\2\2\2j\f\3\2\2\2kl\7k\2\2lm\7i\2\2mn\7p\2\2no\7q\2\2op\7t\2\2p"+
		"x\7g\2\2qr\7K\2\2rs\7I\2\2st\7P\2\2tu\7Q\2\2uv\7T\2\2vx\7G\2\2wk\3\2\2"+
		"\2wq\3\2\2\2x\16\3\2\2\2yz\7c\2\2z~\7u\2\2{|\7C\2\2|~\7U\2\2}y\3\2\2\2"+
		"}{\3\2\2\2~\20\3\2\2\2\177\u0080\7c\2\2\u0080\u0081\7w\2\2\u0081\u0082"+
		"\7v\2\2\u0082\u0083\7q\2\2\u0083\u0084\7e\2\2\u0084\u0085\7t\2\2\u0085"+
		"\u0086\7g\2\2\u0086\u0087\7c\2\2\u0087\u0088\7v\2\2\u0088\u0094\7g\2\2"+
		"\u0089\u008a\7C\2\2\u008a\u008b\7W\2\2\u008b\u008c\7V\2\2\u008c\u008d"+
		"\7Q\2\2\u008d\u008e\7E\2\2\u008e\u008f\7T\2\2\u008f\u0090\7G\2\2\u0090"+
		"\u0091\7C\2\2\u0091\u0092\7V\2\2\u0092\u0094\7G\2\2\u0093\177\3\2\2\2"+
		"\u0093\u0089\3\2\2\2\u0094\22\3\2\2\2\u0095\u0096\7c\2\2\u0096\u0097\7"+
		"w\2\2\u0097\u0098\7v\2\2\u0098\u0099\7q\2\2\u0099\u009a\7g\2\2\u009a\u009b"+
		"\7x\2\2\u009b\u009c\7q\2\2\u009c\u009d\7n\2\2\u009d\u009e\7x\2\2\u009e"+
		"\u00aa\7g\2\2\u009f\u00a0\7C\2\2\u00a0\u00a1\7W\2\2\u00a1\u00a2\7V\2\2"+
		"\u00a2\u00a3\7Q\2\2\u00a3\u00a4\7G\2\2\u00a4\u00a5\7X\2\2\u00a5\u00a6"+
		"\7Q\2\2\u00a6\u00a7\7N\2\2\u00a7\u00a8\7X\2\2\u00a8\u00aa\7G\2\2\u00a9"+
		"\u0095\3\2\2\2\u00a9\u009f\3\2\2\2\u00aa\24\3\2\2\2\u00ab\u00ac\7d\2\2"+
		"\u00ac\u00ad\7c\2\2\u00ad\u00ae\7v\2\2\u00ae\u00af\7e\2\2\u00af\u00b6"+
		"\7j\2\2\u00b0\u00b1\7D\2\2\u00b1\u00b2\7C\2\2\u00b2\u00b3\7V\2\2\u00b3"+
		"\u00b4\7E\2\2\u00b4\u00b6\7J\2\2\u00b5\u00ab\3\2\2\2\u00b5\u00b0\3\2\2"+
		"\2\u00b6\26\3\2\2\2\u00b7\u00b8\7e\2\2\u00b8\u00b9\7c\2\2\u00b9\u00ba"+
		"\7r\2\2\u00ba\u00bb\7k\2\2\u00bb\u00bc\7v\2\2\u00bc\u00bd\7c\2\2\u00bd"+
		"\u00be\7n\2\2\u00be\u00bf\7k\2\2\u00bf\u00c0\7|\2\2\u00c0\u00cc\7g\2\2"+
		"\u00c1\u00c2\7E\2\2\u00c2\u00c3\7C\2\2\u00c3\u00c4\7R\2\2\u00c4\u00c5"+
		"\7K\2\2\u00c5\u00c6\7V\2\2\u00c6\u00c7\7C\2\2\u00c7\u00c8\7N\2\2\u00c8"+
		"\u00c9\7K\2\2\u00c9\u00ca\7\\\2\2\u00ca\u00cc\7G\2\2\u00cb\u00b7\3\2\2"+
		"\2\u00cb\u00c1\3\2\2\2\u00cc\30\3\2\2\2\u00cd\u00ce\7r\2\2\u00ce\u00d2"+
		"\7m\2\2\u00cf\u00d0\7R\2\2\u00d0\u00d2\7M\2\2\u00d1\u00cd\3\2\2\2\u00d1"+
		"\u00cf\3\2\2\2\u00d2\32\3\2\2\2\u00d3\u00d5\4\62;\2\u00d4\u00d3\3\2\2"+
		"\2\u00d5\u00d6\3\2\2\2\u00d6\u00d4\3\2\2\2\u00d6\u00d7\3\2\2\2\u00d7\34"+
		"\3\2\2\2\u00d8\u00d9\7,\2\2\u00d9\36\3\2\2\2\u00da\u00db\7.\2\2\u00db"+
		" \3\2\2\2\u00dc\u00dd\7\60\2\2\u00dd\"\3\2\2\2\u00de\u00e0\t\2\2\2\u00df"+
		"\u00de\3\2\2\2\u00e0\u00e1\3\2\2\2\u00e1\u00df\3\2\2\2\u00e1\u00e2\3\2"+
		"\2\2\u00e2$\3\2\2\2\u00e3\u00e5\t\3\2\2\u00e4\u00e3\3\2\2\2\u00e5\u00e6"+
		"\3\2\2\2\u00e6\u00e4\3\2\2\2\u00e6\u00e7\3\2\2\2\u00e7&\3\2\2\2\u00e8"+
		"\u00ea\7\17\2\2\u00e9\u00e8\3\2\2\2\u00e9\u00ea\3\2\2\2\u00ea\u00eb\3"+
		"\2\2\2\u00eb\u00ec\7\f\2\2\u00ec\u00ed\3\2\2\2\u00ed\u00ee\b\24\2\2\u00ee"+
		"(\3\2\2\2\u00ef\u00f1\t\4\2\2\u00f0\u00ef\3\2\2\2\u00f1\u00f2\3\2\2\2"+
		"\u00f2\u00f0\3\2\2\2\u00f2\u00f3\3\2\2\2\u00f3\u00f4\3\2\2\2\u00f4\u00f5"+
		"\b\25\2\2\u00f5*\3\2\2\2\u00f6\u00f7\7?\2\2\u00f7,\3\2\2\2\24\29GQ_iw"+
		"}\u0093\u00a9\u00b5\u00cb\u00d1\u00d6\u00e1\u00e6\u00e9\u00f2\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}