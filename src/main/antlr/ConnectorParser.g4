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
   : ( UPSERT pk FIELD into)
   ;

write_mode
   : insert_into | upsert_into | upsert_pk_into
   ;

schema_name
   : FIELD
   ;

insert_from_clause
   : write_mode table_name select_clause_basic ( autocreate )? (with_structure)? ( PK primary_key_list)? (with_target)? ( autoevolve )? ( batching )?
    ( capitalize )? ( initialize )? ( project_to )? (partitionby)? (distributeby)? (clusterby)? (timestamp_clause)? (timestamp_unit_clause)?
    ( with_format_clause )? (with_unwrap_clause)? (storeas_clause)? (with_tags)? (with_inc_mode)? (with_type)? (with_doc_type)? (with_index_suffix)?
    (ttl_clause)? (with_converter)? (with_jms_selector)? (with_key)? (key_delimiter)? (with_pipeline_clause)? (with_partitioner_clause)?
    (with_subscription_clause)? (with_compression_clause)? (with_delay_clause)? (with_regex_clause)? (with_flush_size_clause)?
    (with_flush_interval_clause)? (with_flush_records_clause)? (with_schema_evolution_clause)? (with_table_location_clause)? (with_overwrite_clause)?
    (with_partitioning_clause)? (limit_clause)?
   ;

select_clause
   : select_clause_basic ( PK primary_key_list)? (with_structure)? (with_format_clause)? (with_unwrap_clause)? (with_consumer_group)? (with_offset_list)? (sample_clause)? (limit_clause)? (storeas_clause)? (with_tags)? (with_inc_mode)? (with_doc_type)? (with_index_suffix)? (with_converter)? (key_delimiter)?
   ;

select_clause_basic
   : SELECT column_list FROM topic_name ( with_ignore )?
   ;

topic_name
   : ( FIELD | TOPICNAME | DOT )+
   ;

table_name
   : ( FIELD | TOPICNAME | DOT )+
   ;

column_name
   : column ( AS column_name_alias )? | ASTERISK
   ;

column
   : FIELD ( DOT FIELD )* (DOT ASTERISK)?
   ;

column_name_alias
   : FIELD
   ;

column_list
   : column_name ( COMMA column_name )*
   ;

from_clause
   : FROM table_name
   ;

ignored_name
   : FIELD | TOPICNAME
   ;

with_ignore
   : IGNORE ignore_clause
   ;

ignore_clause
   : column_name ( COMMA column_name )*
   ;

pk_name
   : column
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
   : FIELD
   ;

partition_list
   : partition_name ( COMMA partition_name)*
   ;

partitionby
   : PARTITIONBY partition_list
   ;


distribute_name
   : FIELD
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
   : FIELD | SYS_TIME
   ;

timestamp_unit_clause
   : TIMESTAMPUNIT EQUAL timestamp_unit_value
   ;

timestamp_unit_value
   : FIELD
   ;

buckets_number
    : INT
    ;

clusterby_name
    : FIELD
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
    :  INT|FIELD| TOPICNAME
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

limit_clause
    : LIMIT limit_value
    ;

limit_value
    : INT
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

with_unwrap_clause
    : WITHUNWRAP
    ;

with_format_clause
    : WITHFORMAT with_format
    ;

with_structure
    : WITHSTRUCTURE
    ;

with_format
    : FORMAT
    ;

project_to
    : PROJECTTO version_number
    ;

version_number
    : INT
    ;

storeas_clause
    : STOREAS storeas_type (storeas_parameters)*
    ;

storeas_type
    : FIELD | ( DOT | TOPICNAME )+
    ;

storeas_parameters
    : (LEFT_PARAN storeas_parameters_tuple ( COMMA storeas_parameters_tuple)* RIGHT_PARAN)
    ;


storeas_parameters_tuple
    :  storeas_parameter EQUAL storeas_value
    ;

storeas_parameter
    : FIELD | ( DOT | TOPICNAME )+
    ;

storeas_value
    : FIELD | (DOT|TOPICNAME)+ | INT
    ;

with_tags
    : WITHTAG (LEFT_PARAN tag_definition ( COMMA tag_definition )* RIGHT_PARAN)
    ;

with_key
    : WITHKEY (LEFT_PARAN with_key_value (COMMA with_key_value)* RIGHT_PARAN)
    ;

with_key_value
    : FIELD | TOPICNAME | INT
    ;

key_delimiter
    : KEYDELIM  EQUAL key_delimiter_value
    ;

key_delimiter_value
    : KEYDELIMVALUE
    ;

with_inc_mode
    : INCREMENTALMODE EQUAL inc_mode
    ;

inc_mode
    : FIELD | TOPICNAME
    ;

with_type
    : WITHTYPE with_type_value
    ;


with_type_value
    : FIELD
    ;

with_doc_type
    : WITHDOCTYPE EQUAL doc_type
    ;

doc_type
    : FIELD | TOPICNAME | INT
    ;

with_index_suffix
    : WITHINDEXSUFFIX EQUAL index_suffix
    ;

index_suffix
    : FIELD | TOPICNAME | INT
    ;

with_converter
    : WITHCONVERTER EQUAL with_converter_value
    ;

with_converter_value
    : ID | TOPICNAME
    ;

with_target
    : WITHTARGET EQUAL with_target_value
    ;

with_target_value
    : (FIELD|DOT)+ | (DOT|TOPICNAME)+
    ;

with_jms_selector
    : WITHJMSSELECTOR EQUAL jms_selector_value
    ;

jms_selector_value
   : ID | TOPICNAME
   ;

tag_definition
    : tag_key ( (EQUAL tag_value)| (AS tag_value))?
    ;

tag_key
    : (FIELD|DOT)+ | (DOT|TOPICNAME)+
    ;

tag_value
    : FIELD | (DOT|TOPICNAME)+ | INT
    ;


ttl_clause
    : TTL EQUAL ttl_type
    ;

ttl_type
    : INT
    ;

with_pipeline_clause
    : WITHPIPELINE EQUAL pipeline_value
    ;

pipeline_value
    :  (FIELD | (DOT|TOPICNAME)+ | INT)+
    ;

with_partitioner_clause
   : WITHPARTITIONER EQUAL with_partitioner_value
   ;

with_partitioner_value
   : FIELD
   ;

with_subscription_clause
   : WITHSUBSCRIPTION EQUAL with_subscription_value
   ;

with_subscription_value
   : FIELD
   ;

 with_compression_clause
     : WITHCOMPRESSION EQUAL with_compression_type
     ;

 with_compression_type
     : FIELD
     ;

 with_delay_clause
    : WITHDELAY EQUAL with_delay_value
    ;

 with_delay_value
    : INT
    ;

with_regex_clause
    : WITHREGEX EQUAL with_regex_value
    ;

with_regex_value
    : ID | TOPICNAME
    ;


with_flush_size_clause
    : WITH_FLUSH_SIZE EQUAL with_flush_bytes_value
    ;

with_flush_bytes_value
    : INT
    ;

with_flush_interval_clause
    : WITH_FLUSH_INTERVAL EQUAL with_flush_interval_value
    ;

with_flush_interval_value
    : INT
    ;

with_flush_records_clause
    : WITH_FLUSH_COUNT EQUAL with_flush_records_value
    ;

with_flush_records_value
    : INT
    ;

with_schema_evolution_clause
    : WITH_SCHEMA_EVOLUTION EQUAL with_schema_evolution_value
    ;

with_schema_evolution_value
    : FIELD
    ;

with_table_location_clause
    : WITH_TABLE_LOCATION EQUAL with_table_location_value
    ;

with_table_location_value
    : ID | TOPICNAME
    ;

with_overwrite_clause
    : WITH_OVERWRITE
    ;

with_partitioning_clause
    : WITH_PARTITIONING EQUAL with_partitioning_value
    ;

with_partitioning_value
    : FIELD
    ;

