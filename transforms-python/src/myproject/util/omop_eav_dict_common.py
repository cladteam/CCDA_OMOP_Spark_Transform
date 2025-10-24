from pyspark.sql import types as T
from collections import defaultdict  

from ..util.ds_schema import domain_key_fields


# Ultimate EAV or RDF triple
omop_dict_schema = T.StructType([
    T.StructField('domain_name', T.StringType(), True),
    T.StructField('key_type', T.StringType(), True),
    T.StructField('key_value', T.StringType(), True),
    T.StructField('field_name', T.StringType(), True),
    T.StructField('field_value', T.StringType(), True)
])

def concat_key(domain_name, record_dict):
    return domain_name + "|" +  "|".join(list(map(str, record_dict.values())))

def lookup_key_value(domain_name, record_dict):
   return domain_name + "|" + record_dict[domain_key_fields[domain_name]]

def flatten_and_stringify_record_dict(domain_name, record_dict):
    key_type = 'concat_key'
    record_key = concat_key(domain_name, record_dict)
    eav_list = []
    for key, value in record_dict.items():
        eav_list.append({
            'domain_name': domain_name,
            'key_type': key_type,
            'key_value': record_key,
            'field_name': key,
            'field_value': str(value)
            })
    return eav_list


def get_codemap_dict_list(codemap_ds):
    narrow = codemap_ds.dataframe().select(['src_vocab_code_system', 'src_code', 'source_concept_id', 
                                            'target_domain_id',    'target_concept_id']).collect()
    codemap_dict = defaultdict(list)
    for row in narrow:
        key = (row['src_vocab_code_system'], row['src_code'])
        value = {
            'source_concept_id': row['source_concept_id'],
            'target_domain_id': row['target_domain_id'],
            'target_concept_id': row['target_concept_id']
        }
        codemap_dict[key].append(value)  # Append to list
    return codemap_dict

def get_valueset_dict_list(valuemap_ds):
    narrow = valuemap_ds.dataframe().select(['codeSystem', 'src_cd', 'target_domain_id', 'target_concept_id']).collect()
    valuemap_dict = defaultdict(list)
    for row in narrow:
        key = (row['codeSystem'], row['src_cd'])
        value = {
            'source_concept_id': None,
            'target_domain_id': row['target_domain_id'],
            'target_concept_id': row['target_concept_id']
        }
        valuemap_dict[key].append(value)  # Append to list
    return valuemap_dict


def get_visitmap_dict_list(visitmap_ds):
    narrow = visitmap_ds.dataframe().select(['codeSystem', 'src_cd', 'target_domain_id', 'target_concept_id']).collect()
    visitmap_dict = defaultdict(list)
    for row in narrow:
        key = (row['codeSystem'], row['src_cd'])
        value = {
            'source_concept_id': None,
            'target_domain_id': row['target_domain_id'],
            'target_concept_id': row['target_concept_id']
        }
        visitmap_dict[key].append(value)
    return visitmap_dict