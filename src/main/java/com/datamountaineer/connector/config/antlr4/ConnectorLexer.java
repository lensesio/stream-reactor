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
		DISTRIBUTEBY=15, TIMESTAMP=16, STOREDAS=17, SYS_TIME=18, PK=19, INT=20, 
		ASTERISK=21, COMMA=22, DOT=23, ID=24, TOPICNAME=25, NEWLINE=26, WS=27, 
		EQUAL=28;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "PARTITIONBY", 
		"DISTRIBUTEBY", "TIMESTAMP", "STOREDAS", "SYS_TIME", "PK", "INT", "ASTERISK", 
		"COMMA", "DOT", "ID", "TOPICNAME", "NEWLINE", "WS", "EQUAL"
	};

	private static final String[] _LITERAL_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, "'*'", "','", "'.'", 
		null, null, null, null, "'='"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, "INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "PARTITIONBY", 
		"DISTRIBUTEBY", "TIMESTAMP", "STOREDAS", "SYS_TIME", "PK", "INT", "ASTERISK", 
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\36\u01a0\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\3\2\3\2\3\2\3\2\3\2\3\2"+
		"\3\2\3\2\3\2\3\2\3\2\3\2\5\2H\n\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
		"\3\3\3\3\3\3\5\3V\n\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4`\n\4\3\5\3\5"+
		"\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5n\n\5\3\6\3\6\3\6\3\6\3\6"+
		"\3\6\3\6\3\6\5\6x\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7"+
		"\5\7\u0086\n\7\3\b\3\b\3\b\3\b\5\b\u008c\n\b\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u00a2\n\t\3"+
		"\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n"+
		"\3\n\3\n\5\n\u00b8\n\n\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3"+
		"\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\5\13\u00cc\n\13\3\f\3\f\3"+
		"\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u00dc\n\f\3\r\3\r\3"+
		"\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\5\r\u00e8\n\r\3\16\3\16\3\16\3\16\3\16"+
		"\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16"+
		"\3\16\5\16\u00fe\n\16\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
		"\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\5\17\u0116"+
		"\n\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\5\20\u0130\n\20"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21"+
		"\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\3\21\5\21\u014c"+
		"\n\21\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22\3\22"+
		"\3\22\3\22\3\22\5\22\u015e\n\22\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23"+
		"\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\3\23\5\23\u0174"+
		"\n\23\3\24\3\24\3\24\3\24\5\24\u017a\n\24\3\25\6\25\u017d\n\25\r\25\16"+
		"\25\u017e\3\26\3\26\3\27\3\27\3\30\3\30\3\31\6\31\u0188\n\31\r\31\16\31"+
		"\u0189\3\32\6\32\u018d\n\32\r\32\16\32\u018e\3\33\5\33\u0192\n\33\3\33"+
		"\3\33\3\33\3\33\3\34\6\34\u0199\n\34\r\34\16\34\u019a\3\34\3\34\3\35\3"+
		"\35\2\2\36\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33"+
		"\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67"+
		"\359\36\3\2\5\6\2\62;C\\aac|\b\2--/\60\62;C\\aac|\5\2\13\f\17\17\"\"\u01b7"+
		"\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2"+
		"\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2"+
		"\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2"+
		"\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2"+
		"\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\3G\3"+
		"\2\2\2\5U\3\2\2\2\7_\3\2\2\2\tm\3\2\2\2\13w\3\2\2\2\r\u0085\3\2\2\2\17"+
		"\u008b\3\2\2\2\21\u00a1\3\2\2\2\23\u00b7\3\2\2\2\25\u00cb\3\2\2\2\27\u00db"+
		"\3\2\2\2\31\u00e7\3\2\2\2\33\u00fd\3\2\2\2\35\u0115\3\2\2\2\37\u012f\3"+
		"\2\2\2!\u014b\3\2\2\2#\u015d\3\2\2\2%\u0173\3\2\2\2\'\u0179\3\2\2\2)\u017c"+
		"\3\2\2\2+\u0180\3\2\2\2-\u0182\3\2\2\2/\u0184\3\2\2\2\61\u0187\3\2\2\2"+
		"\63\u018c\3\2\2\2\65\u0191\3\2\2\2\67\u0198\3\2\2\29\u019e\3\2\2\2;<\7"+
		"k\2\2<=\7p\2\2=>\7u\2\2>?\7g\2\2?@\7t\2\2@H\7v\2\2AB\7K\2\2BC\7P\2\2C"+
		"D\7U\2\2DE\7G\2\2EF\7T\2\2FH\7V\2\2G;\3\2\2\2GA\3\2\2\2H\4\3\2\2\2IJ\7"+
		"w\2\2JK\7r\2\2KL\7u\2\2LM\7g\2\2MN\7t\2\2NV\7v\2\2OP\7W\2\2PQ\7R\2\2Q"+
		"R\7U\2\2RS\7G\2\2ST\7T\2\2TV\7V\2\2UI\3\2\2\2UO\3\2\2\2V\6\3\2\2\2WX\7"+
		"k\2\2XY\7p\2\2YZ\7v\2\2Z`\7q\2\2[\\\7K\2\2\\]\7P\2\2]^\7V\2\2^`\7Q\2\2"+
		"_W\3\2\2\2_[\3\2\2\2`\b\3\2\2\2ab\7u\2\2bc\7g\2\2cd\7n\2\2de\7g\2\2ef"+
		"\7e\2\2fn\7v\2\2gh\7U\2\2hi\7G\2\2ij\7N\2\2jk\7G\2\2kl\7E\2\2ln\7V\2\2"+
		"ma\3\2\2\2mg\3\2\2\2n\n\3\2\2\2op\7h\2\2pq\7t\2\2qr\7q\2\2rx\7o\2\2st"+
		"\7H\2\2tu\7T\2\2uv\7Q\2\2vx\7O\2\2wo\3\2\2\2ws\3\2\2\2x\f\3\2\2\2yz\7"+
		"k\2\2z{\7i\2\2{|\7p\2\2|}\7q\2\2}~\7t\2\2~\u0086\7g\2\2\177\u0080\7K\2"+
		"\2\u0080\u0081\7I\2\2\u0081\u0082\7P\2\2\u0082\u0083\7Q\2\2\u0083\u0084"+
		"\7T\2\2\u0084\u0086\7G\2\2\u0085y\3\2\2\2\u0085\177\3\2\2\2\u0086\16\3"+
		"\2\2\2\u0087\u0088\7c\2\2\u0088\u008c\7u\2\2\u0089\u008a\7C\2\2\u008a"+
		"\u008c\7U\2\2\u008b\u0087\3\2\2\2\u008b\u0089\3\2\2\2\u008c\20\3\2\2\2"+
		"\u008d\u008e\7c\2\2\u008e\u008f\7w\2\2\u008f\u0090\7v\2\2\u0090\u0091"+
		"\7q\2\2\u0091\u0092\7e\2\2\u0092\u0093\7t\2\2\u0093\u0094\7g\2\2\u0094"+
		"\u0095\7c\2\2\u0095\u0096\7v\2\2\u0096\u00a2\7g\2\2\u0097\u0098\7C\2\2"+
		"\u0098\u0099\7W\2\2\u0099\u009a\7V\2\2\u009a\u009b\7Q\2\2\u009b\u009c"+
		"\7E\2\2\u009c\u009d\7T\2\2\u009d\u009e\7G\2\2\u009e\u009f\7C\2\2\u009f"+
		"\u00a0\7V\2\2\u00a0\u00a2\7G\2\2\u00a1\u008d\3\2\2\2\u00a1\u0097\3\2\2"+
		"\2\u00a2\22\3\2\2\2\u00a3\u00a4\7c\2\2\u00a4\u00a5\7w\2\2\u00a5\u00a6"+
		"\7v\2\2\u00a6\u00a7\7q\2\2\u00a7\u00a8\7g\2\2\u00a8\u00a9\7x\2\2\u00a9"+
		"\u00aa\7q\2\2\u00aa\u00ab\7n\2\2\u00ab\u00ac\7x\2\2\u00ac\u00b8\7g\2\2"+
		"\u00ad\u00ae\7C\2\2\u00ae\u00af\7W\2\2\u00af\u00b0\7V\2\2\u00b0\u00b1"+
		"\7Q\2\2\u00b1\u00b2\7G\2\2\u00b2\u00b3\7X\2\2\u00b3\u00b4\7Q\2\2\u00b4"+
		"\u00b5\7N\2\2\u00b5\u00b6\7X\2\2\u00b6\u00b8\7G\2\2\u00b7\u00a3\3\2\2"+
		"\2\u00b7\u00ad\3\2\2\2\u00b8\24\3\2\2\2\u00b9\u00ba\7e\2\2\u00ba\u00bb"+
		"\7n\2\2\u00bb\u00bc\7w\2\2\u00bc\u00bd\7u\2\2\u00bd\u00be\7v\2\2\u00be"+
		"\u00bf\7g\2\2\u00bf\u00c0\7t\2\2\u00c0\u00c1\7d\2\2\u00c1\u00cc\7{\2\2"+
		"\u00c2\u00c3\7E\2\2\u00c3\u00c4\7N\2\2\u00c4\u00c5\7W\2\2\u00c5\u00c6"+
		"\7U\2\2\u00c6\u00c7\7V\2\2\u00c7\u00c8\7G\2\2\u00c8\u00c9\7T\2\2\u00c9"+
		"\u00ca\7D\2\2\u00ca\u00cc\7[\2\2\u00cb\u00b9\3\2\2\2\u00cb\u00c2\3\2\2"+
		"\2\u00cc\26\3\2\2\2\u00cd\u00ce\7d\2\2\u00ce\u00cf\7w\2\2\u00cf\u00d0"+
		"\7e\2\2\u00d0\u00d1\7m\2\2\u00d1\u00d2\7g\2\2\u00d2\u00d3\7v\2\2\u00d3"+
		"\u00dc\7u\2\2\u00d4\u00d5\7D\2\2\u00d5\u00d6\7W\2\2\u00d6\u00d7\7E\2\2"+
		"\u00d7\u00d8\7M\2\2\u00d8\u00d9\7G\2\2\u00d9\u00da\7V\2\2\u00da\u00dc"+
		"\7U\2\2\u00db\u00cd\3\2\2\2\u00db\u00d4\3\2\2\2\u00dc\30\3\2\2\2\u00dd"+
		"\u00de\7d\2\2\u00de\u00df\7c\2\2\u00df\u00e0\7v\2\2\u00e0\u00e1\7e\2\2"+
		"\u00e1\u00e8\7j\2\2\u00e2\u00e3\7D\2\2\u00e3\u00e4\7C\2\2\u00e4\u00e5"+
		"\7V\2\2\u00e5\u00e6\7E\2\2\u00e6\u00e8\7J\2\2\u00e7\u00dd\3\2\2\2\u00e7"+
		"\u00e2\3\2\2\2\u00e8\32\3\2\2\2\u00e9\u00ea\7e\2\2\u00ea\u00eb\7c\2\2"+
		"\u00eb\u00ec\7r\2\2\u00ec\u00ed\7k\2\2\u00ed\u00ee\7v\2\2\u00ee\u00ef"+
		"\7c\2\2\u00ef\u00f0\7n\2\2\u00f0\u00f1\7k\2\2\u00f1\u00f2\7|\2\2\u00f2"+
		"\u00fe\7g\2\2\u00f3\u00f4\7E\2\2\u00f4\u00f5\7C\2\2\u00f5\u00f6\7R\2\2"+
		"\u00f6\u00f7\7K\2\2\u00f7\u00f8\7V\2\2\u00f8\u00f9\7C\2\2\u00f9\u00fa"+
		"\7N\2\2\u00fa\u00fb\7K\2\2\u00fb\u00fc\7\\\2\2\u00fc\u00fe\7G\2\2\u00fd"+
		"\u00e9\3\2\2\2\u00fd\u00f3\3\2\2\2\u00fe\34\3\2\2\2\u00ff\u0100\7r\2\2"+
		"\u0100\u0101\7c\2\2\u0101\u0102\7t\2\2\u0102\u0103\7v\2\2\u0103\u0104"+
		"\7k\2\2\u0104\u0105\7v\2\2\u0105\u0106\7k\2\2\u0106\u0107\7q\2\2\u0107"+
		"\u0108\7p\2\2\u0108\u0109\7d\2\2\u0109\u0116\7{\2\2\u010a\u010b\7R\2\2"+
		"\u010b\u010c\7C\2\2\u010c\u010d\7T\2\2\u010d\u010e\7V\2\2\u010e\u010f"+
		"\7K\2\2\u010f\u0110\7V\2\2\u0110\u0111\7K\2\2\u0111\u0112\7Q\2\2\u0112"+
		"\u0113\7P\2\2\u0113\u0114\7D\2\2\u0114\u0116\7[\2\2\u0115\u00ff\3\2\2"+
		"\2\u0115\u010a\3\2\2\2\u0116\36\3\2\2\2\u0117\u0118\7f\2\2\u0118\u0119"+
		"\7k\2\2\u0119\u011a\7u\2\2\u011a\u011b\7v\2\2\u011b\u011c\7t\2\2\u011c"+
		"\u011d\7k\2\2\u011d\u011e\7d\2\2\u011e\u011f\7w\2\2\u011f\u0120\7v\2\2"+
		"\u0120\u0121\7g\2\2\u0121\u0122\7d\2\2\u0122\u0130\7{\2\2\u0123\u0124"+
		"\7F\2\2\u0124\u0125\7K\2\2\u0125\u0126\7U\2\2\u0126\u0127\7V\2\2\u0127"+
		"\u0128\7T\2\2\u0128\u0129\7K\2\2\u0129\u012a\7D\2\2\u012a\u012b\7W\2\2"+
		"\u012b\u012c\7V\2\2\u012c\u012d\7G\2\2\u012d\u012e\7D\2\2\u012e\u0130"+
		"\7[\2\2\u012f\u0117\3\2\2\2\u012f\u0123\3\2\2\2\u0130 \3\2\2\2\u0131\u0132"+
		"\7y\2\2\u0132\u0133\7k\2\2\u0133\u0134\7v\2\2\u0134\u0135\7j\2\2\u0135"+
		"\u0136\7v\2\2\u0136\u0137\7k\2\2\u0137\u0138\7o\2\2\u0138\u0139\7g\2\2"+
		"\u0139\u013a\7u\2\2\u013a\u013b\7v\2\2\u013b\u013c\7c\2\2\u013c\u013d"+
		"\7o\2\2\u013d\u014c\7r\2\2\u013e\u013f\7Y\2\2\u013f\u0140\7K\2\2\u0140"+
		"\u0141\7V\2\2\u0141\u0142\7J\2\2\u0142\u0143\7V\2\2\u0143\u0144\7K\2\2"+
		"\u0144\u0145\7O\2\2\u0145\u0146\7G\2\2\u0146\u0147\7U\2\2\u0147\u0148"+
		"\7V\2\2\u0148\u0149\7C\2\2\u0149\u014a\7O\2\2\u014a\u014c\7R\2\2\u014b"+
		"\u0131\3\2\2\2\u014b\u013e\3\2\2\2\u014c\"\3\2\2\2\u014d\u014e\7u\2\2"+
		"\u014e\u014f\7v\2\2\u014f\u0150\7q\2\2\u0150\u0151\7t\2\2\u0151\u0152"+
		"\7g\2\2\u0152\u0153\7f\2\2\u0153\u0154\7c\2\2\u0154\u015e\7u\2\2\u0155"+
		"\u0156\7U\2\2\u0156\u0157\7V\2\2\u0157\u0158\7Q\2\2\u0158\u0159\7T\2\2"+
		"\u0159\u015a\7G\2\2\u015a\u015b\7F\2\2\u015b\u015c\7C\2\2\u015c\u015e"+
		"\7U\2\2\u015d\u014d\3\2\2\2\u015d\u0155\3\2\2\2\u015e$\3\2\2\2\u015f\u0160"+
		"\7u\2\2\u0160\u0161\7{\2\2\u0161\u0162\7u\2\2\u0162\u0163\7a\2\2\u0163"+
		"\u0164\7v\2\2\u0164\u0165\7k\2\2\u0165\u0166\7o\2\2\u0166\u0167\7g\2\2"+
		"\u0167\u0168\7*\2\2\u0168\u0174\7+\2\2\u0169\u016a\7U\2\2\u016a\u016b"+
		"\7[\2\2\u016b\u016c\7U\2\2\u016c\u016d\7a\2\2\u016d\u016e\7V\2\2\u016e"+
		"\u016f\7K\2\2\u016f\u0170\7O\2\2\u0170\u0171\7G\2\2\u0171\u0172\7*\2\2"+
		"\u0172\u0174\7+\2\2\u0173\u015f\3\2\2\2\u0173\u0169\3\2\2\2\u0174&\3\2"+
		"\2\2\u0175\u0176\7r\2\2\u0176\u017a\7m\2\2\u0177\u0178\7R\2\2\u0178\u017a"+
		"\7M\2\2\u0179\u0175\3\2\2\2\u0179\u0177\3\2\2\2\u017a(\3\2\2\2\u017b\u017d"+
		"\4\62;\2\u017c\u017b\3\2\2\2\u017d\u017e\3\2\2\2\u017e\u017c\3\2\2\2\u017e"+
		"\u017f\3\2\2\2\u017f*\3\2\2\2\u0180\u0181\7,\2\2\u0181,\3\2\2\2\u0182"+
		"\u0183\7.\2\2\u0183.\3\2\2\2\u0184\u0185\7\60\2\2\u0185\60\3\2\2\2\u0186"+
		"\u0188\t\2\2\2\u0187\u0186\3\2\2\2\u0188\u0189\3\2\2\2\u0189\u0187\3\2"+
		"\2\2\u0189\u018a\3\2\2\2\u018a\62\3\2\2\2\u018b\u018d\t\3\2\2\u018c\u018b"+
		"\3\2\2\2\u018d\u018e\3\2\2\2\u018e\u018c\3\2\2\2\u018e\u018f\3\2\2\2\u018f"+
		"\64\3\2\2\2\u0190\u0192\7\17\2\2\u0191\u0190\3\2\2\2\u0191\u0192\3\2\2"+
		"\2\u0192\u0193\3\2\2\2\u0193\u0194\7\f\2\2\u0194\u0195\3\2\2\2\u0195\u0196"+
		"\b\33\2\2\u0196\66\3\2\2\2\u0197\u0199\t\4\2\2\u0198\u0197\3\2\2\2\u0199"+
		"\u019a\3\2\2\2\u019a\u0198\3\2\2\2\u019a\u019b\3\2\2\2\u019b\u019c\3\2"+
		"\2\2\u019c\u019d\b\34\2\2\u019d8\3\2\2\2\u019e\u019f\7?\2\2\u019f:\3\2"+
		"\2\2\33\2GU_mw\u0085\u008b\u00a1\u00b7\u00cb\u00db\u00e7\u00fd\u0115\u012f"+
		"\u014b\u015d\u0173\u0179\u017e\u0189\u018e\u0191\u019a\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}