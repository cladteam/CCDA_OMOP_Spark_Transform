
from transforms.api import transform, Input, Output, configure
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import Row
import numpy as np

import io
##import logger
import datetime
from numpy import datetime64
import os
import pandas as pd
import re
import time

from prototype_2 import layer_datasets
from prototype_2 import codemap_xwalk
from prototype_2 import ccda_value_set_mapping_table_dataset
from prototype_2 import visit_concept_xwalk_mapping_dataset
from prototype_2 import ddl
from ..util.ds_schema import domain_dataset_schema
from ..util.correct_types import correct_types_in_record_list
from prototype_2.ddl import config_to_domain_name_dict
from prototype_2.ddl import domain_name_to_table_name
import prototype_2.data_driven_parse as DDP
from prototype_2.domain_dataframe_column_types import domain_dataframe_column_types
# `LongType()` can not accept object `3055738571586194` in type `int64`.



#
# READ "Distributed processing" here: https://foundry.cladplatform.org/docs/foundry/transforms-python/unstructured-files/
#
# NB The code in here still doesn't deal with the issue that we're creating multiple types of rows 
# for each input file. At this point, I'm just doing the Person table to see if we're able to
# distribute processing.



def just_convert(ctx, domain_name, dict_of_lists_by_domain):
    if domain_name in dict_of_lists_by_domain:
        schema = domain_dataset_schema[domain_name]
        domain_list = dict_of_lists_by_domain[domain_name]
        if domain_list:
            correct_types_in_record_list(domain_name, domain_list)
            spark_dff = ctx.spark_session.createDataFrame(domain_list, schema) 
            return spark_dff

    return None


@transform(
    care_site = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/care_site"),
    condition_occurrence = Output("ri.foundry.main.dataset.e34c8928-d1c1-4b4e-8026-e6024e6afdbb"),
    drug_exposure = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/drug_exposure"),
    location = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/location"),
    measurement = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/measurement"),
    observation = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/observation"),
    person = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/person"),
    procedure_occurrence = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/procedure_occurrence"),
    provider = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/provider"),
    visit_occurrence = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/visit_occurrence"),

    xml_files=Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_cedars_response_files"),
    # xml_files=Input("ri.foundry.main.dataset.ca873ab5-748b-4f53-9ae4-0c819c7fa3d4"),
    metadata = Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata"),
    visit_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
    codemap_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk"),
    valueset_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset"),
)
def compute(ctx,
    care_site, condition_occurrence,  drug_exposure, location, 
    measurement, observation, person,
    procedure_occurrence, provider, visit_occurrence, 
    xml_files,
    metadata, visit_xwalk_ds, codemap_xwalk_ds, valueset_xwalk_ds
    ):
    FILE_LIMIT=0
        
    global codemap_xwalk
    global ccda_value_set_mapping_table_dataset
    global visit_concept_xwalk_mapping_dataset
    codemap_xwalk = codemap_xwalk_ds
    ccda_value_set_mapping_table_dataset = valueset_xwalk_ds
    visit_concept_xwalk_mapping_dataset = visit_xwalk_ds

    filestatus_list = list(xml_files.filesystem().ls())
    omop_dict = {}
    accumulating_domain_rows_dict = {} 
    fs = xml_files.filesystem()
    doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)

    domain_dfs = {
        'Condition': condition_occurrence,
        'Care_Site': care_site,
        'Drug': drug_exposure,
        'Location': location,
        'Measurement': measurement,
        'Observation': observation,
        'Person': person,
        'Procedure': procedure_occurrence,
        'Provider': provider,
        'Visit': visit_occurrence
    } 

    def process_file(file_status):
        # Q: what actually IS the input here? In the other code I have to call 
        #      df.filesystem().ls() to get the list of file status things
        # yields a dictionary of lists of Rows
        msg = f"WHAT TYPE {type(file_status)} {file_status}"
        raise Exception(msg)

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

                new_dict = layer_datasets.process_string_to_dict(match_tuple[0], file_status.path, False )

                #for domain_name in new_dict.keys():
                #    for dict in new_dict[domain_name]:
                #        yield(Row(**dict))
                # Just try to make this work on Person, a single schema
                for dict in new_dict['Person']:
                    yield(Row(**dict))


    for domain_name in domain_dfs.keys(): 
        # flatMap() takes a file, a function to process it, and returns an (single) RDD
        # the problem is that process_file returns may different kinds of rows.
        # How much distribution magic is built into flatMap()????

        #rdd = xml_files.rdd.flatMap(process_file)
        rdd = xml_files.dataframe().flatMap(process_file)
        processed_df  = rdd.toDF(domain_dataset_schema(domain_name))
        domain_dfs[domain_name].write_dataframe(processed_df)

        
    #just_convert(ctx, domain_name, new_dict) ???????????????????????????
        #filestatus_list = list(xml_files.filesystem().ls())
        #fs = xml_files.filesystem()




