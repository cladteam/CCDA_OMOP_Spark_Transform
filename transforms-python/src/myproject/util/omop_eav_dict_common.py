
from pyspark.sql import types as T


from ..util.ds_schema import domain_key_fields


# Ultimate EAV or RDF triple
# (Entity-Attribute-Value (EAV), the name given to RDF style triples in relational databases)
# Instead of having a row with an ID field and multiple attributes, you have many rows each with and ID
# and one of the attributes.
omop_dict_schema = T.StructType([
    T.StructField('domain_name', T.StringType(), True),                     # 1
    T.StructField('key_type', T.StringType(), True),                     # 1
    T.StructField('key_value', T.StringType(), True),                     # 1
    T.StructField('field_name', T.StringType(), True),                     # 1
    T.StructField('field_value', T.StringType(), True)                     # 1
])

def concat_key(domain_name, record_dict):
    return domain_name + "|" +  "|".join(list(map(str, record_dict.values())))

def lookup_key_value(domain_name, record_dict):
   # absolutely needs for the OMOP data coming out of data_driven_parse.py to have unique IDs
   # and that's broken for the moment.
   return domain_name + "|" + record_dict[domain_key_fields[domain_name]]

def flatten_and_stringify_record_dict(domain_name, record_dict):
    # key_type = 'lookup_key_value'
    key_type = 'concat_key'
    record_key = concat_key(domain_name, record_dict)
    # record_key = lookup_key_value(record_dict)
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


def get_codemap_dict(codemap_ds):
    #  df = codemap_xwalk[ (codemap_xwalk['src_vocab_code_system'] == vocabulary_oid) & (codemap_xwalk['src_code']  == concept_code) ]
    #  'source_concept_id, 'target_domain_id','target_concept_id'
    narrow = codemap_ds.dataframe().select(['src_vocab_code_system', 'src_code', 'source_concept_id', 'target_domain_id', 'target_concept_id']).collect()
    codemap_dict = {}
    for row in narrow:
        codemap_dict[(row['src_vocab_code_system'], row['src_code'])] = {
            'source_concept_id': row['source_concept_id'],
            'target_domain_id': row['target_domain_id'],
            'target_concept_id': row['target_concept_id'] }

    return codemap_dict

def get_valueset_dict(codemap_ds):
    narrow = codemap_ds.dataframe().select(['codeSystem', 'src_cd', 'target_domain_id', 'target_concept_id']).collect()
    codemap_dict = {}
    for row in narrow:
        codemap_dict[(row['codeSystem'], row['src_cd'])] = {
            'source_concept_id': None,
            'target_domain_id': row['target_domain_id'],
            'target_concept_id': row['target_concept_id'] }

    return codemap_dict


def get_visit_dict(codemap_ds):
    narrow = codemap_ds.dataframe().select(['codeSystem', 'src_cd', 'target_domain_id', 'target_concept_id']).collect()
    codemap_dict = {}
    for row in narrow:
        codemap_dict[(row['codeSystem'], row['src_cd'])] = {
            'source_concept_id': None,
            'target_domain_id': row['target_domain_id'],
            'target_concept_id': row['target_concept_id'] }

    return codemap_dict

