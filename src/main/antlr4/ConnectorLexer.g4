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

PK
   : 'pk' | 'PK'
   ;

ERRPOLICY
   : 'noop' | 'NOOP' | 'retry' | 'RETRY' | 'throw' | 'THROW'
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


INT
   : '0' .. '9'+
   ;


NEWLINE
   : '\r'? '\n' -> skip
   ;


WS
   : ( ' ' | '\t' | '\n' | '\r' )+ -> skip
   ;
