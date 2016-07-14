lexer grammar ConnectorLexer;
@ header {
 }

INSERT
   : 'insert' | 'INSERT'
   ;

UPSERT
   : 'upsert' | 'UPSERT'
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

CLUSTERBY
    : 'clusterby' | 'CLUSTERBY'
    ;

BUCKETS
    : 'buckets'|'BUCKETS'
    ;

BATCH
   : 'batch' | 'BATCH'
   ;

CAPITALIZE
   : 'capitalize' | 'CAPITALIZE'
   ;

PARTITIONBY
   : 'partitionby' | 'PARTITIONBY'
   ;

DISTRIBUTEBY
   : 'distributeby' | 'DISTRIBUTEBY'
   ;

PK
   : 'pk' | 'PK'
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


ID
   : ( 'a' .. 'z' | 'A' .. 'Z' | '_' | '0' .. '9' )+
   ;


TOPICNAME
   : ID (('.'? '-'? '+'? ID)+)?
   ;


NEWLINE
   : '\r'? '\n' -> skip
   ;


WS
   : ( ' ' | '\t' | '\n' | '\r' )+ -> skip
   ;


EQUAL
   : '='
   ;
