lexer grammar ConnectorLexer;
@ header {
 }

SQL_ACTION
   : 'insert' | 'upsert' | 'INSERT' | 'UPSERT'
   ;


SELECT
   : 'select' | 'SELECT'
   ;


FROM
   : 'from' | 'FROM'
   ;


WHERE
   : 'where' | 'WHERE'
   ;


AND
   : 'and' | 'AND' | '&&'
   ;


OR
   : 'or' | 'OR' | '||'
   ;


XOR
   : 'xor' | 'XOR'
   ;


IS
   : 'is' | 'IS'
   ;


NULL
   : 'null' | 'NULL'
   ;


LIKE
   : 'like' | 'LIKE'
   ;


IN
   : 'in' | 'IN'
   ;


EXISTS
   : 'exists' | 'EXISTS'
   ;


ALL
   : 'all' | 'ALL'
   ;


ANY
   : 'any' | 'ANY'
   ;


TRUE
   : 'true' | 'TRUE'
   ;


FALSE
   : 'false' | 'FALSE'
   ;


DIVIDE
   : 'div' | '/' | 'DIV'
   ;


MOD
   : 'mod' | '%' | 'MOD'
   ;


BETWEEN
   : 'between' | 'BETWEEN'
   ;


REGEXP
   : 'regexp' | 'REGEXP'
   ;


PLUS
   : '+'
   ;


MINUS
   : '-'
   ;


NEGATION
   : '~'
   ;


VERTBAR
   : '|'
   ;


BITAND
   : '&'
   ;


POWER_OP
   : '^'
   ;


BINARY
   : 'binary'
   ;


SHIFT_LEFT
   : '<<'
   ;


SHIFT_RIGHT
   : '>>'
   ;


ESCAPE
   : 'escape'
   ;


ASTERISK
   : '*'
   ;


RPAREN
   : ')'
   ;


LPAREN
   : '('
   ;


RBRACK
   : ']'
   ;


LBRACK
   : '['
   ;


COLON
   : ':'
   ;


ALL_FIELDS
   : '.*'
   ;


EQ
   : '='
   ;


LTH
   : '<'
   ;


GTH
   : '>'
   ;


NOT_EQ
   : '!='
   ;


NOT
   : 'not'
   ;


LET
   : '<='
   ;


GET
   : '>='
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


COLLATE
   : 'collate'
   ;


INNER
   : 'inner'
   ;


OUTER
   : 'outer'
   ;


JOIN
   : 'join'
   ;


CROSS
   : 'cross'
   ;


USING
   : 'using'
   ;


INDEX
   : 'index'
   ;


KEY
   : 'key'
   ;


ORDER
   : 'order'
   ;


GROUP
   : 'group'
   ;


BY
   : 'by'
   ;


FOR
   : 'for'
   ;


USE
   : 'use'
   ;


IGNORE
   : 'ignore'
   ;


PARTITION
   : 'partition'
   ;


STRAIGHT_JOIN
   : 'straight_join'
   ;


NATURAL
   : 'natural'
   ;


LEFT
   : 'left'
   ;


RIGHT
   : 'right'
   ;


OJ
   : 'oj'
   ;


ON
   : 'on'
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


USER_VAR
   : '@' ( USER_VAR_SUBFIX1 | USER_VAR_SUBFIX2 | USER_VAR_SUBFIX3 | USER_VAR_SUBFIX4 )
   ;


fragment USER_VAR_SUBFIX1
   : ( '`' ( ~ '`' )+ '`' )
   ;


fragment USER_VAR_SUBFIX2
   : ( '\'' ( ~ '\'' )+ '\'' )
   ;


fragment USER_VAR_SUBFIX3
   : ( '\"' ( ~ '\"' )+ '\"' )
   ;


fragment USER_VAR_SUBFIX4
   : ( 'A' .. 'Z' | 'a' .. 'z' | '_' | '$' | '0' .. '9' | DOT )+
   ;