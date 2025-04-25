
from transforms.api import transform, Input, Output, configure
from pyspark.sql import types as T
from pyspark.sql import Row

import io
import re

from ..util.correct_types import correct_types_in_record_list
from ..util.ds_schema import domain_key_fields
from prototype_2 import layer_datasets
from prototype_2.domain_dataframe_column_types import domain_dataframe_column_types
from prototype_2 import ddl
from ..util.omop_eav_dict_common import omop_dict_schema 
from ..util.omop_eav_dict_common import concat_key
from ..util.omop_eav_dict_common import lookup_key_value
from ..util.omop_eav_dict_common import flatten_and_stringify_record_dict
from ..util.omop_eav_dict_common import get_codemap_dict
from ..util.omop_eav_dict_common import get_valueset_dict
from ..util.omop_eav_dict_common import get_visit_dict

### @configure(profile=['DRIVER_MEMORY_EXTRA_LARGE', 'DRIVER_MEMORY_OVERHEAD_LARGE', 'EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_64' ])
##@configure(profile=['DRIVER_MEMORY_EXTRA_LARGE', 'EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_64'])
#@configure(profile=['DRIVER_MEMORY_EXTRA_LARGE', 'EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_16'])



# https://stackoverflow.com/questions/70792919/how-do-i-know-my-foundry-job-is-using-aqe

#@transform(
#    omop_eav_dict = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/omop_eav_dict"),
#
#    xml_files=Input("ri.foundry.main.dataset.119054ed-4719-4d84-99ba-43625bcafd0f"),
#    visit_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
#    ## codemap_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk"),
#    valueset_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset"),
#)
def compute(ctx, omop_eav_dict, 
    xml_files,
    visit_xwalk_ds, 
    ## codemap_xwalk_ds, 
    valueset_xwalk_ds ):

    ## codemap_dict = get_codemap_dict(codemap_xwalk_ds)
    value_set_map_dict = get_valueset_dict(valueset_xwalk_ds)
    visit_map_dict = get_visit_dict(visit_xwalk_ds)


    doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)
    fs = xml_files.filesystem()

    ## codemap_broadcast = ctx.spark_session.sparkContext.broadcast(codemap_dict)  # BROADCAST
    visitmap_broadcast = ctx.spark_session.sparkContext.broadcast(value_set_map_dict)  # BROADCAST
    valuemap_broadcast = ctx.spark_session.sparkContext.broadcast(visit_map_dict)  # BROADCAST

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

                new_dict = layer_datasets.process_string_to_dict_no_codemap(\
                    xml_content, file_status.path, False, \
                    ## codemap_broadcast.value, 
                    visitmap_broadcast.value, valuemap_broadcast.value )  

                for config_name in new_dict.keys():
                    if new_dict[config_name] is not None:
                        domain_name = ddl.config_to_domain_name_dict[config_name]
                        correct_types_in_record_list(domain_name, new_dict[config_name])
                        for record_dict in new_dict[config_name]:
                            eav_list = flatten_and_stringify_record_dict(domain_name, record_dict)
                            for eav_record in eav_list:
                                yield(Row(**eav_record))


# NOTE THE LIMIT!!!
    #files_df = xml_files.filesystem().files('**/*.xml').limit(10)  # 11m11s 
    ###files_df = xml_files.filesystem().files('**/*.xml').limit(100) # 18m48s with 16 executors
    files_df = xml_files.filesystem().files('**/*.xml').limit(100) # 18m48s with 16 executors
    #files_df = xml_files.filesystem().files('**/*.xml').limit(1000) # 1h 10m 2s
    #files_df = xml_files.filesystem().files('**/*.xml') # ~7000
    rdd = files_df.rdd.flatMap(process_file)
    processed_df = rdd.toDF(omop_dict_schema)
    omop_eav_dict.write_dataframe(processed_df) 
