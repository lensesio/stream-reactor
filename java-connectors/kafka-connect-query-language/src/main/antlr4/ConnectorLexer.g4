lexer grammar ConnectorLexer;
@ header {
 }

INSERT
   : 'insert' | 'INSERT'
   ;

UPSERT
   : 'upsert' | 'UPSERT'
   ;

UPDATE
   : 'update' | 'UPDATE'
   ;

INTO
   : 'into' | 'INTO'
   ;

SELECT
   : 'select' | 'SELECT'
   ;


FROM
   : 'from' | 'FROM'
   ;


IGNORE
   : 'ignore' | 'IGNORE'
   ;

AS
   : 'as' | 'AS'
   ;

AUTOCREATE
   : 'autocreate' | 'AUTOCREATE'
   ;


AUTOEVOLVE
   : 'autoevolve' | 'AUTOEVOLVE'
   ;

BATCH
   : 'batch' | 'BATCH'
   ;

PARTITIONBY
   : 'partitionby' | 'PARTITIONBY'
   ;

TIMESTAMP
    : 'withtimestamp' | 'WITHTIMESTAMP'
    ;

SYS_TIME
    : 'sys_time()' | 'SYS_TIME()'
    ;

WITHTAG
    :   'withtag' | 'WITHTAG'
    ;

WITHKEY
    :   'withkey' | 'WITHKEY'
    ;

KEYDELIM
    :  'keydelimiter' | 'KEYDELIMITER'
    ;

WITHSTRUCTURE
    : 'withstructure' | 'WITHSTRUCTURE'
    ;

WITHTYPE
    : 'withtype' | 'WITHTYPE'
    ;

PK
   : 'pk' | 'PK'
   ;

WITHFORMAT
    : 'WITHFORMAT'|'withformat'
    ;

WITHUNWRAP
    : 'WITHUNWRAP'| 'withunwrap'
    ;

STOREAS
    : 'STOREAS'|'storeas'
    ;

LIMIT
    : 'LIMIT' | 'limit'
    ;

INCREMENTALMODE
    : 'INCREMENTALMODE'|'incrementalmode'
    ;

WITHDOCTYPE
    :  'WITHDOCTYPE'|'withdoctype'
    ;

WITHINDEXSUFFIX
    : 'WITHINDEXSUFFIX' | 'withindexsuffix'
    ;

WITHCONVERTER
    : 'WITHCONVERTER' | 'withconverter'
    ;

WITHJMSSELECTOR
    : 'WITHJMSSELECTOR' | 'withjmsselector'
    ;

WITHTARGET
    : 'WITHTARGET' | 'withtarget'
    ;

WITHSUBSCRIPTION
    : 'WITHSUBSCRIPTION' | 'withsubscription'
    ;

TIMESTAMPUNIT
    : 'TIMESTAMPUNIT' | 'timestampunit'
    ;

WITHPIPELINE
    : 'WITHPIPELINE' | 'withpipeline'
    ;

WITHREGEX
    : 'WITHREGEX'|'withregex'
    ;

TTL
   : 'TTL'|'ttl'
   ;

 PROPERTIES
    : 'properties' | 'PROPERTIES'
    ;

EQUAL
   : '='
   ;

INT
   : '0' .. '9'+
   ;

ASTERISK
   : '*'
   ;

COMMA
   : ','
   ;

DOT
   : '.'
   ;

LEFT_PARAN
    : '('
    ;

RIGHT_PARAN
    : ')'
    ;

FIELD
   : ( 'a' .. 'z' | 'A' .. 'Z' | '@' |'_' | '0' .. '9' )+
   ;


TOPICNAME
   : ( 'a' .. 'z' | 'A' .. 'Z' | '_' | '0' .. '9' | '-' | '+' | '/' |'{'|'}'|':' )+ | ESCAPED_TOPIC
   ;

KEYDELIMVALUE
   : '\'' ('a' .. 'z' | 'A' .. 'Z' | '_' | '0' .. '9' | '-' | '+' | '/' |'{'|'}'|':'|'|'|'#'|'@'|'`'|'^'|'['|']'|'*'|'?'|'$') '\''
   ;



fragment ESCAPED_TOPIC
    : ( '`' (~'`')+ '`')
    ;

ESCAPED_FIELD
    : FIELD DOT ( '`' (~'`')+ '`')
    ;

STRING: '\'' ~('\'' | '\r' | '\n')* '\'';

NEWLINE
   : '\r'? '\n' -> skip
   ;


WS
   : ( ' ' | '\t' | '\n' | '\r' )+ -> skip
   ;

