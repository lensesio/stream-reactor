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
   : sql_action table_name select_clause_basic ( autocreate )? ( PK primary_key_list)? ( autoevolve )? ( batching )? ( capitalize )? ( initialize )? (partitionby)? (distributeby)? (clusterby)? (timestamp_clause)? ( with_format_clause )?
   ;

select_clause
   : select_clause_basic with_format_clause (with_consumer_group)? (with_offset_list)? (sample_clause)?
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

initialize
   : INITIALIZE
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
    :  INT|ID|TOPICNAME
    ;


offset_partition_inner
    : INT (COMMA INT)?
    ;

offset_partition
    : (LEFT_PARAN offset_partition_inner RIGHT_PARAN)
    ;

partition_offset_list
    : offset_partition ( COMMA offset_partition )*
    ;

with_offset_list
    : WITHOFFSET partition_offset_list
    ;


sample_clause
    : SAMPLE sample_value EVERY sample_period
    ;

sample_value
    : INT
    ;

sample_period
    : INT
    ;

with_format_clause
    : WITHFORMAT with_format
    ;

with_format
    : FORMAT
    ;