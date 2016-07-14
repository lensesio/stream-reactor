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
		DISTRIBUTEBY=15, PK=16, INT=17, ASTERISK=18, COMMA=19, DOT=20, ID=21, 
		TOPICNAME=22, NEWLINE=23, WS=24, EQUAL=25;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"INSERT", "UPSERT", "INTO", "SELECT", "FROM", "IGNORE", "AS", "AUTOCREATE", 
		"AUTOEVOLVE", "CLUSTERBY", "BUCKETS", "BATCH", "CAPITALIZE", "PARTITIONBY", 
		"DISTRIBUTEBY", "PK", "INT", "ASTERISK", "COMMA", "DOT", "ID", "TOPICNAME", 
		"NEWLINE", "WS", "EQUAL"
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
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\33\u0162\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\5\2B\n"+
		"\2\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3P\n\3\3\4\3\4\3"+
		"\4\3\4\3\4\3\4\3\4\3\4\5\4Z\n\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3"+
		"\5\3\5\3\5\5\5h\n\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6r\n\6\3\7\3\7\3"+
		"\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\5\7\u0080\n\7\3\b\3\b\3\b\3\b\5"+
		"\b\u0086\n\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"+
		"\t\3\t\3\t\3\t\3\t\3\t\5\t\u009c\n\t\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3"+
		"\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\3\n\5\n\u00b2\n\n\3\13\3\13"+
		"\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13"+
		"\3\13\3\13\5\13\u00c6\n\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f"+
		"\3\f\3\f\3\f\5\f\u00d6\n\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\3\r\5\r"+
		"\u00e2\n\r\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16"+
		"\3\16\3\16\3\16\3\16\3\16\3\16\3\16\3\16\5\16\u00f8\n\16\3\17\3\17\3\17"+
		"\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17"+
		"\3\17\3\17\3\17\3\17\3\17\5\17\u0110\n\17\3\20\3\20\3\20\3\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20"+
		"\3\20\3\20\3\20\3\20\5\20\u012a\n\20\3\21\3\21\3\21\3\21\5\21\u0130\n"+
		"\21\3\22\6\22\u0133\n\22\r\22\16\22\u0134\3\23\3\23\3\24\3\24\3\25\3\25"+
		"\3\26\6\26\u013e\n\26\r\26\16\26\u013f\3\27\3\27\5\27\u0144\n\27\3\27"+
		"\5\27\u0147\n\27\3\27\5\27\u014a\n\27\3\27\6\27\u014d\n\27\r\27\16\27"+
		"\u014e\5\27\u0151\n\27\3\30\5\30\u0154\n\30\3\30\3\30\3\30\3\30\3\31\6"+
		"\31\u015b\n\31\r\31\16\31\u015c\3\31\3\31\3\32\3\32\2\2\33\3\3\5\4\7\5"+
		"\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23"+
		"%\24\'\25)\26+\27-\30/\31\61\32\63\33\3\2\4\6\2\62;C\\aac|\5\2\13\f\17"+
		"\17\"\"\u017a\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2"+
		"\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2"+
		"\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2"+
		"\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2"+
		"\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\3A\3\2\2\2\5O\3\2\2\2\7Y\3\2\2"+
		"\2\tg\3\2\2\2\13q\3\2\2\2\r\177\3\2\2\2\17\u0085\3\2\2\2\21\u009b\3\2"+
		"\2\2\23\u00b1\3\2\2\2\25\u00c5\3\2\2\2\27\u00d5\3\2\2\2\31\u00e1\3\2\2"+
		"\2\33\u00f7\3\2\2\2\35\u010f\3\2\2\2\37\u0129\3\2\2\2!\u012f\3\2\2\2#"+
		"\u0132\3\2\2\2%\u0136\3\2\2\2\'\u0138\3\2\2\2)\u013a\3\2\2\2+\u013d\3"+
		"\2\2\2-\u0141\3\2\2\2/\u0153\3\2\2\2\61\u015a\3\2\2\2\63\u0160\3\2\2\2"+
		"\65\66\7k\2\2\66\67\7p\2\2\678\7u\2\289\7g\2\29:\7t\2\2:B\7v\2\2;<\7K"+
		"\2\2<=\7P\2\2=>\7U\2\2>?\7G\2\2?@\7T\2\2@B\7V\2\2A\65\3\2\2\2A;\3\2\2"+
		"\2B\4\3\2\2\2CD\7w\2\2DE\7r\2\2EF\7u\2\2FG\7g\2\2GH\7t\2\2HP\7v\2\2IJ"+
		"\7W\2\2JK\7R\2\2KL\7U\2\2LM\7G\2\2MN\7T\2\2NP\7V\2\2OC\3\2\2\2OI\3\2\2"+
		"\2P\6\3\2\2\2QR\7k\2\2RS\7p\2\2ST\7v\2\2TZ\7q\2\2UV\7K\2\2VW\7P\2\2WX"+
		"\7V\2\2XZ\7Q\2\2YQ\3\2\2\2YU\3\2\2\2Z\b\3\2\2\2[\\\7u\2\2\\]\7g\2\2]^"+
		"\7n\2\2^_\7g\2\2_`\7e\2\2`h\7v\2\2ab\7U\2\2bc\7G\2\2cd\7N\2\2de\7G\2\2"+
		"ef\7E\2\2fh\7V\2\2g[\3\2\2\2ga\3\2\2\2h\n\3\2\2\2ij\7h\2\2jk\7t\2\2kl"+
		"\7q\2\2lr\7o\2\2mn\7H\2\2no\7T\2\2op\7Q\2\2pr\7O\2\2qi\3\2\2\2qm\3\2\2"+
		"\2r\f\3\2\2\2st\7k\2\2tu\7i\2\2uv\7p\2\2vw\7q\2\2wx\7t\2\2x\u0080\7g\2"+
		"\2yz\7K\2\2z{\7I\2\2{|\7P\2\2|}\7Q\2\2}~\7T\2\2~\u0080\7G\2\2\177s\3\2"+
		"\2\2\177y\3\2\2\2\u0080\16\3\2\2\2\u0081\u0082\7c\2\2\u0082\u0086\7u\2"+
		"\2\u0083\u0084\7C\2\2\u0084\u0086\7U\2\2\u0085\u0081\3\2\2\2\u0085\u0083"+
		"\3\2\2\2\u0086\20\3\2\2\2\u0087\u0088\7c\2\2\u0088\u0089\7w\2\2\u0089"+
		"\u008a\7v\2\2\u008a\u008b\7q\2\2\u008b\u008c\7e\2\2\u008c\u008d\7t\2\2"+
		"\u008d\u008e\7g\2\2\u008e\u008f\7c\2\2\u008f\u0090\7v\2\2\u0090\u009c"+
		"\7g\2\2\u0091\u0092\7C\2\2\u0092\u0093\7W\2\2\u0093\u0094\7V\2\2\u0094"+
		"\u0095\7Q\2\2\u0095\u0096\7E\2\2\u0096\u0097\7T\2\2\u0097\u0098\7G\2\2"+
		"\u0098\u0099\7C\2\2\u0099\u009a\7V\2\2\u009a\u009c\7G\2\2\u009b\u0087"+
		"\3\2\2\2\u009b\u0091\3\2\2\2\u009c\22\3\2\2\2\u009d\u009e\7c\2\2\u009e"+
		"\u009f\7w\2\2\u009f\u00a0\7v\2\2\u00a0\u00a1\7q\2\2\u00a1\u00a2\7g\2\2"+
		"\u00a2\u00a3\7x\2\2\u00a3\u00a4\7q\2\2\u00a4\u00a5\7n\2\2\u00a5\u00a6"+
		"\7x\2\2\u00a6\u00b2\7g\2\2\u00a7\u00a8\7C\2\2\u00a8\u00a9\7W\2\2\u00a9"+
		"\u00aa\7V\2\2\u00aa\u00ab\7Q\2\2\u00ab\u00ac\7G\2\2\u00ac\u00ad\7X\2\2"+
		"\u00ad\u00ae\7Q\2\2\u00ae\u00af\7N\2\2\u00af\u00b0\7X\2\2\u00b0\u00b2"+
		"\7G\2\2\u00b1\u009d\3\2\2\2\u00b1\u00a7\3\2\2\2\u00b2\24\3\2\2\2\u00b3"+
		"\u00b4\7e\2\2\u00b4\u00b5\7n\2\2\u00b5\u00b6\7w\2\2\u00b6\u00b7\7u\2\2"+
		"\u00b7\u00b8\7v\2\2\u00b8\u00b9\7g\2\2\u00b9\u00ba\7t\2\2\u00ba\u00bb"+
		"\7d\2\2\u00bb\u00c6\7{\2\2\u00bc\u00bd\7E\2\2\u00bd\u00be\7N\2\2\u00be"+
		"\u00bf\7W\2\2\u00bf\u00c0\7U\2\2\u00c0\u00c1\7V\2\2\u00c1\u00c2\7G\2\2"+
		"\u00c2\u00c3\7T\2\2\u00c3\u00c4\7D\2\2\u00c4\u00c6\7[\2\2\u00c5\u00b3"+
		"\3\2\2\2\u00c5\u00bc\3\2\2\2\u00c6\26\3\2\2\2\u00c7\u00c8\7d\2\2\u00c8"+
		"\u00c9\7w\2\2\u00c9\u00ca\7e\2\2\u00ca\u00cb\7m\2\2\u00cb\u00cc\7g\2\2"+
		"\u00cc\u00cd\7v\2\2\u00cd\u00d6\7u\2\2\u00ce\u00cf\7D\2\2\u00cf\u00d0"+
		"\7W\2\2\u00d0\u00d1\7E\2\2\u00d1\u00d2\7M\2\2\u00d2\u00d3\7G\2\2\u00d3"+
		"\u00d4\7V\2\2\u00d4\u00d6\7U\2\2\u00d5\u00c7\3\2\2\2\u00d5\u00ce\3\2\2"+
		"\2\u00d6\30\3\2\2\2\u00d7\u00d8\7d\2\2\u00d8\u00d9\7c\2\2\u00d9\u00da"+
		"\7v\2\2\u00da\u00db\7e\2\2\u00db\u00e2\7j\2\2\u00dc\u00dd\7D\2\2\u00dd"+
		"\u00de\7C\2\2\u00de\u00df\7V\2\2\u00df\u00e0\7E\2\2\u00e0\u00e2\7J\2\2"+
		"\u00e1\u00d7\3\2\2\2\u00e1\u00dc\3\2\2\2\u00e2\32\3\2\2\2\u00e3\u00e4"+
		"\7e\2\2\u00e4\u00e5\7c\2\2\u00e5\u00e6\7r\2\2\u00e6\u00e7\7k\2\2\u00e7"+
		"\u00e8\7v\2\2\u00e8\u00e9\7c\2\2\u00e9\u00ea\7n\2\2\u00ea\u00eb\7k\2\2"+
		"\u00eb\u00ec\7|\2\2\u00ec\u00f8\7g\2\2\u00ed\u00ee\7E\2\2\u00ee\u00ef"+
		"\7C\2\2\u00ef\u00f0\7R\2\2\u00f0\u00f1\7K\2\2\u00f1\u00f2\7V\2\2\u00f2"+
		"\u00f3\7C\2\2\u00f3\u00f4\7N\2\2\u00f4\u00f5\7K\2\2\u00f5\u00f6\7\\\2"+
		"\2\u00f6\u00f8\7G\2\2\u00f7\u00e3\3\2\2\2\u00f7\u00ed\3\2\2\2\u00f8\34"+
		"\3\2\2\2\u00f9\u00fa\7r\2\2\u00fa\u00fb\7c\2\2\u00fb\u00fc\7t\2\2\u00fc"+
		"\u00fd\7v\2\2\u00fd\u00fe\7k\2\2\u00fe\u00ff\7v\2\2\u00ff\u0100\7k\2\2"+
		"\u0100\u0101\7q\2\2\u0101\u0102\7p\2\2\u0102\u0103\7d\2\2\u0103\u0110"+
		"\7{\2\2\u0104\u0105\7R\2\2\u0105\u0106\7C\2\2\u0106\u0107\7T\2\2\u0107"+
		"\u0108\7V\2\2\u0108\u0109\7K\2\2\u0109\u010a\7V\2\2\u010a\u010b\7K\2\2"+
		"\u010b\u010c\7Q\2\2\u010c\u010d\7P\2\2\u010d\u010e\7D\2\2\u010e\u0110"+
		"\7[\2\2\u010f\u00f9\3\2\2\2\u010f\u0104\3\2\2\2\u0110\36\3\2\2\2\u0111"+
		"\u0112\7f\2\2\u0112\u0113\7k\2\2\u0113\u0114\7u\2\2\u0114\u0115\7v\2\2"+
		"\u0115\u0116\7t\2\2\u0116\u0117\7k\2\2\u0117\u0118\7d\2\2\u0118\u0119"+
		"\7w\2\2\u0119\u011a\7v\2\2\u011a\u011b\7g\2\2\u011b\u011c\7d\2\2\u011c"+
		"\u012a\7{\2\2\u011d\u011e\7F\2\2\u011e\u011f\7K\2\2\u011f\u0120\7U\2\2"+
		"\u0120\u0121\7V\2\2\u0121\u0122\7T\2\2\u0122\u0123\7K\2\2\u0123\u0124"+
		"\7D\2\2\u0124\u0125\7W\2\2\u0125\u0126\7V\2\2\u0126\u0127\7G\2\2\u0127"+
		"\u0128\7D\2\2\u0128\u012a\7[\2\2\u0129\u0111\3\2\2\2\u0129\u011d\3\2\2"+
		"\2\u012a \3\2\2\2\u012b\u012c\7r\2\2\u012c\u0130\7m\2\2\u012d\u012e\7"+
		"R\2\2\u012e\u0130\7M\2\2\u012f\u012b\3\2\2\2\u012f\u012d\3\2\2\2\u0130"+
		"\"\3\2\2\2\u0131\u0133\4\62;\2\u0132\u0131\3\2\2\2\u0133\u0134\3\2\2\2"+
		"\u0134\u0132\3\2\2\2\u0134\u0135\3\2\2\2\u0135$\3\2\2\2\u0136\u0137\7"+
		",\2\2\u0137&\3\2\2\2\u0138\u0139\7.\2\2\u0139(\3\2\2\2\u013a\u013b\7\60"+
		"\2\2\u013b*\3\2\2\2\u013c\u013e\t\2\2\2\u013d\u013c\3\2\2\2\u013e\u013f"+
		"\3\2\2\2\u013f\u013d\3\2\2\2\u013f\u0140\3\2\2\2\u0140,\3\2\2\2\u0141"+
		"\u0150\5+\26\2\u0142\u0144\7\60\2\2\u0143\u0142\3\2\2\2\u0143\u0144\3"+
		"\2\2\2\u0144\u0146\3\2\2\2\u0145\u0147\7/\2\2\u0146\u0145\3\2\2\2\u0146"+
		"\u0147\3\2\2\2\u0147\u0149\3\2\2\2\u0148\u014a\7-\2\2\u0149\u0148\3\2"+
		"\2\2\u0149\u014a\3\2\2\2\u014a\u014b\3\2\2\2\u014b\u014d\5+\26\2\u014c"+
		"\u0143\3\2\2\2\u014d\u014e\3\2\2\2\u014e\u014c\3\2\2\2\u014e\u014f\3\2"+
		"\2\2\u014f\u0151\3\2\2\2\u0150\u014c\3\2\2\2\u0150\u0151\3\2\2\2\u0151"+
		".\3\2\2\2\u0152\u0154\7\17\2\2\u0153\u0152\3\2\2\2\u0153\u0154\3\2\2\2"+
		"\u0154\u0155\3\2\2\2\u0155\u0156\7\f\2\2\u0156\u0157\3\2\2\2\u0157\u0158"+
		"\b\30\2\2\u0158\60\3\2\2\2\u0159\u015b\t\3\2\2\u015a\u0159\3\2\2\2\u015b"+
		"\u015c\3\2\2\2\u015c\u015a\3\2\2\2\u015c\u015d\3\2\2\2\u015d\u015e\3\2"+
		"\2\2\u015e\u015f\b\31\2\2\u015f\62\3\2\2\2\u0160\u0161\7?\2\2\u0161\64"+
		"\3\2\2\2\34\2AOYgq\177\u0085\u009b\u00b1\u00c5\u00d5\u00e1\u00f7\u010f"+
		"\u0129\u012f\u0134\u013f\u0143\u0146\u0149\u014e\u0150\u0153\u015c\3\b"+
		"\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}