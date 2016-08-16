parser grammar ConnectorParser;

options
   { tokenVocab = ConnectorLexer; }

stat
   : insert_from_clause|select_clause
   ;

into
   : INTO
   ;

pk
   : PK
   ;

insert_into
   : INSERT into
   ;

upsert_into
   : UPSERT into
   ;

upsert_pk_into
   : UPSERT pk ID into
   ;

sql_action
   : insert_into | upsert_into | upsert_pk_into
   ;

schema_name
   : ID
   ;

insert_from_clause
   : sql_action table_name select_clause_basic ( autocreate )? ( PK primary_key_list)? ( autoevolve )? ( batching )? ( capitalize )? (partitionby)? (distributeby)? (clusterby)? (timestamp_clause)? ( storedas_name )?
   ;

select_clause
   : select_clause_basic (with_consumer_group)? (from_offset_list)?
   ;

select_clause_basic
   : SELECT column_list FROM topic_name ( IGNORE ignore_clause )?
   ;

topic_name
   : ID | TOPICNAME
   ;

table_name
   : ID | TOPICNAME
   ;

column_name
   : ID ( AS column_name_alias )? | ASTERISK
   ;

column_name_alias
   : ID
   ;

column_list
   : column_name ( COMMA column_name )*
   ;

from_clause
   : FROM table_name
   ;

ignored_name
   : ID
   ;

ignore_clause
   : ignored_name ( COMMA ignored_name )*
   ;

pk_name
   : ID
   ;

primary_key_list
   : pk_name ( COMMA pk_name )*
   ;

autocreate
   : AUTOCREATE
   ;

autoevolve
   : AUTOEVOLVE
   ;

batch_size
    : INT
    ;

batching
   : BATCH EQUAL batch_size
   ;

capitalize
   : CAPITALIZE
   ;

partition_name
   : ID
   ;

partition_list
   : partition_name ( COMMA partition_name)*
   ;

partitionby
   : PARTITIONBY partition_list
   ;


distribute_name
   : ID
   ;

distribute_list
   : distribute_name ( COMMA distribute_name)*
   ;

distributeby
   : DISTRIBUTEBY distribute_list INTO buckets_number BUCKETS
   ;

timestamp_clause
   : TIMESTAMP timestamp_value
   ;

timestamp_value
   : ID | SYS_TIME
   ;

storedas_name
   : STOREDAS storedas_value
   ;

storedas_value
   : ID
   ;

buckets_number
    : INT
    ;

clusterby_name
    : ID
    ;

clusterby_list
    :  clusterby_name ( COMMA clusterby_name)*
    ;

clusterby
    : CLUSTERBY clusterby_list INTO buckets_number BUCKETS
    ;

with_consumer_group
    :  WITHGROUP with_consumer_group_value
    ;

with_consumer_group_value
    :  TOPICNAME
    ;

offset_partition
    : WS+partition_value EQUAL offset_value
    ;

partition_value
    : INT
    ;

offset_value
    : INT
    ;

from_offset_list
    : FROMOFFSET offset_partition ( COMMA offset_partition )*
    ;
