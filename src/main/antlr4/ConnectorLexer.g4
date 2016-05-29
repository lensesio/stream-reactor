lexer grammar ConnectorLexer;
@ header {
 }

INSERT
   : 'insert' | 'INSERT'
   ;

UPSERT
   : 'upsert' | 'UPSERT'
   ;

PK
   : 'PK' | 'pk'
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

ASTERISK
   : '*'
   ;


ALL_FIELDS
   : '.*'
   ;

SEMI
   : ';'
   ;


COMMA
   : ','
   ;


DOT
   : '.'
   ;

ID
   : ( 'a' .. 'z' | 'A' .. 'Z' | '_' )+
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
