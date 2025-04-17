
from transforms.api import transform, Input, Output, configure
from pyspark.sql import types as T
from pyspark.sql import Row

import io
# import logger
import re

from prototype_2 import value_transformations
from prototype_2 import layer_datasets
from prototype_2 import set_codemap_xwalk
from prototype_2 import set_ccda_value_set_mapping_table_dataset
from prototype_2 import set_visit_concept_xwalk_mapping_dataset
from ..util.correct_types import correct_types_in_record_list
from ..util.ds_schema import domain_key_fields
from prototype_2.domain_dataframe_column_types import domain_dataframe_column_types
from prototype_2 import ddl


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


@configure(profile=['DRIVER_MEMORY_EXTRA_LARGE', 'DRIVER_MEMORY_OVERHEAD_LARGE', 'NUM_EXECUTORS_64' ])
# https://stackoverflow.com/questions/70792919/how-do-i-know-my-foundry-job-is-using-aqe

@transform(
    omop_eav_dict = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/omop_eav_dict"),

    #xml_files=Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_cedars_response_files"),
    #xml_files=Input("ri.foundry.main.dataset.ca873ab5-748b-4f53-9ae4-0c819c7fa3d4"),
    xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
    metadata = Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata"),
    visit_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
    codemap_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk"),
    valueset_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset"),
)
def compute(ctx, omop_eav_dict, xml_files,
    metadata, visit_xwalk_ds, codemap_xwalk_ds, valueset_xwalk_ds ):

    # THIS DID NOT WORK?
    #global codemap_xwalk
    #global ccda_value_set_mapping_table_dataset
    #global visit_concept_xwalk_mapping_dataset
    #codemap_xwalk = codemap_xwalk_ds
    #ccda_value_set_mapping_table_dataset = valueset_xwalk_ds
    #visit_concept_xwalk_mapping_dataset = visit_xwalk_ds

    # TRY THIS
    set_codemap_xwalk(codemap_xwalk_ds.dataframe())
    set_ccda_value_set_mapping_table_dataset(valueset_xwalk_ds.dataframe())
    set_visit_concept_xwalk_mapping_dataset(visit_xwalk_ds.dataframe())

    # TEST: here outside the flatmap, running on the director
    test_value = value_transformations.codemap_xwalk_concept_id({'vocabulary_oid': '2.16.840.1.113883.6.96', 'concept_code': '608837004', 'default': 'XXX'})
    if test_value is None or test_value == 'XXX' or test_value == 'None':
        raise Exception("codemap_xwalk test failed with some form of None")
    if test_value != '1340204': # 1,340,204
        msg = f"codemap_xwalk test failed to deliver correct code {test_value} {type(test_value)}"
        raise Exception(msg)

    doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)
    fs = xml_files.filesystem()

    def process_file(file_status):
        with fs.open(file_status.path, 'rb') as f:
            br = io.BufferedReader(f)
            tw = io.TextIOWrapper(br) 
            contents = tw.readline()
            for line in tw:
                contents += line
            # Basically selecting content between ClincalDocument tags, looping in case > 1
            for match in doc_regex.finditer(contents):
                match_tuple = match.groups(0)
                xml_content = match_tuple[0]

                new_dict = layer_datasets.process_string_to_dict(xml_content, file_status.path, False )

                for config_name in new_dict.keys():
                    if new_dict[config_name] is not None:
                        domain_name = ddl.config_to_domain_name_dict[config_name]
                        correct_types_in_record_list(domain_name, new_dict[config_name])
                        for record_dict in new_dict[config_name]:
                            eav_list = flatten_and_stringify_record_dict(domain_name, record_dict)
                            for eav_record in eav_list:
                                yield(Row(**eav_record))

### NOTE THE LIMIT
    files_df = xml_files.filesystem().files('**/*.xml').limit(10)
    rdd = files_df.rdd.flatMap(process_file)
    processed_df = rdd.toDF(omop_dict_schema)
    omop_eav_dict.write_dataframe(processed_df) 
