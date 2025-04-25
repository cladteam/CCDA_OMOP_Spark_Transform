from transforms.api import Input, Output, transform, configure, incremental
from pyspark.sql import Row

import io
import re

from ..util.correct_types import correct_types_in_record_list
from prototype_2 import layer_datasets
from prototype_2 import ddl

from ..util.omop_eav_dict_common import omop_dict_schema 
from ..util.omop_eav_dict_common import concat_key
from ..util.omop_eav_dict_common import lookup_key_value
from ..util.omop_eav_dict_common import flatten_and_stringify_record_dict
from ..util.omop_eav_dict_common import get_codemap_dict
from ..util.omop_eav_dict_common import get_valueset_dict
from ..util.omop_eav_dict_common import get_visit_dict

STEP_SIZE=100

""" Most Simple: 0
    No batches
    No broadcast variables
    No flatmp
"""    

#@configure(profile=['DRIVER_MEMORY_EXTRA_LARGE', 'EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_16'])
#@transform(
#    omop_eav_dict = Output("/All of Us-cdb225/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/omop_eav_dict_simple"),
#    input_files=Input("ri.foundry.main.dataset.119054ed-4719-4d84-99ba-43625bcafd0f"),
#    visit_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
#    valueset_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset"),
#)
def compute(ctx, omop_eav_dict, input_files, visit_xwalk_ds, valueset_xwalk_ds):

    value_set_map_dict = get_valueset_dict(valueset_xwalk_ds)
    visit_map_dict = get_visit_dict(visit_xwalk_ds)

    doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)
    input_fs = input_files.filesystem()

    def process_file(file_path): # different from the flatMap way that uses a file status and yields
        """
            Returns a list of dictionaries/records/sme_review_notes
        """
        total_eav_list = []
        with input_fs.open(file_path, 'rb') as f:
            contents = f.read().decode('utf-8')
            #br = io.BufferedReader(f)
            #tw = io.TextIOWrapper(br) 
            #contents = tw.readline()
            #for line in tw:
            #    contents += line
            # Basically selecting content between ClincalDocument tags, looping in case > 1
            for match in doc_regex.finditer(contents):
                match_tuple = match.groups(0)
                xml_content = match_tuple[0]

                new_dict = layer_datasets.process_string_to_dict_no_codemap(\
                    xml_content, file_path, False, \
                    visit_map_dict, value_set_map_dict)

                for config_name in new_dict.keys():
                    if new_dict[config_name] is not None:
                        domain_name = ddl.config_to_domain_name_dict[config_name]
                        correct_types_in_record_list(domain_name, new_dict[config_name])
                        for record_dict in new_dict[config_name]:
                            eav_list = flatten_and_stringify_record_dict(domain_name, record_dict)
                            total_eav_list.extend(eav_list)
        return total_eav_list


    input_files_df = input_fs.files()
    input_files_df = input_files_df.limit(STEP_SIZE)

    input_files_list = input_files_df.collect()
    super_eav_list=[]
    for file_row in input_files_list:
        eav_list = process_file(file_row.path)
        super_eav_list.extend(eav_list)

    omop_eav_df = ctx.spark_session.createDataFrame(super_eav_list, omop_dict_schema)
    omop_eav_dict.write_dataframe(omop_eav_df)











