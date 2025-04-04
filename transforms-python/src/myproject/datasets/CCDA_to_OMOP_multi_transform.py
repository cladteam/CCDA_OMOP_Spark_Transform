
from transforms.api import transform, Input, Output, configure
from pyspark.sql import types as T
from pyspark.sql import functions as F
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
from prototype_2.ddl import config_to_domain_name_dict
from prototype_2.ddl import domain_name_to_table_name
import prototype_2.data_driven_parse as DDP
from prototype_2.domain_dataframe_column_types import domain_dataframe_column_types
# `LongType()` can not accept object `3055738571586194` in type `int64`.

def correct_types_in_record_list(domain_name, record_list):
    table_name = domain_name_to_table_name[domain_name]

    if table_name in domain_dataframe_column_types.keys():
        for column_name, intended_column_type in domain_dataframe_column_types[table_name].items():

            for record in record_list:
                field_value = record[column_name]

                # FROM STRINGS
                if type(field_value) is str:
                    if type(field_value) is str and intended_column_type is int:
                        record[column_name] = int(field_value)
                        if type(record[column_name]) is not intended_column_type:
                            msg=f"CAST to int FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                            raise Exception(msg)

                    if type(field_value) is str and intended_column_type == np.int64:
                        record[column_name] = int(field_value)
                        if type(record[column_name]) is not int:
                            msg=f"CAST for int64 to int FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                            raise Exception(msg)

                    if type(field_value) is str and intended_column_type == np.int32:
                        intermediate_int32 = np.int32(field_value) # hopes of forcing an int, not a long
                        record[column_name] = int(intermediate_int32)
                        if type(record[column_name]) is not int:
                            msg=f"CAST for int64 to int FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                            raise Exception(msg)

                # FROM various INTs, just pass an int, not int32 or int64, but go via int32 when that is asked for
                elif type(field_value) in (int, np.int32, np.int64):
                    if intended_column_type == np.int32:
                        intermediate_int32 = np.int32(field_value) # hopes of forcing an int, not a long
                        record[column_name] = int(intermediate_int32)
                        if type(record[column_name]) is not int:
                            msg=f"CAST for int32 to int FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                            raise Exception(msg)
                    elif intended_column_type == np.int64:
                        record[column_name] = int(field_value)
                        if type(record[column_name]) is not int:
                            msg=f"CAST for int64 to int FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                            raise Exception(msg)
                    else:
                        if type(record[column_name]) is not int:
                            msg=f"something FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                            raise Exception(msg)

                # FROM float
                elif type(field_value) in (np.float32, float, np.float64):
                    if intended_column_type == np.float32:
                        record[column_name] = float(field_value)
                        if type(record[column_name]) is not float:
                            msg=f"CAST for int32 to int FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                            raise Exception(msg)
                    else:
                        if type(record[column_name]) is not float:
                            msg=f"something float FAILED:  cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(record[column_name])} val:{record[column_name]}"
                            raise Exception(msg)


                # Check and throw/raise if incorrect
                # ASSUME NONE IS OK
                field_value = record[column_name]
                if intended_column_type is not np.int64 and intended_column_type is not np.int32 and intended_column_type is not np.float64 and intended_column_type is not np.float32:
                    if field_value is not None and type(field_value) is not  intended_column_type:
                        msg=f"data and intended types don't agree cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
                        raise Exception(msg)
                elif intended_column_type == np.int64:
                    if field_value is not None and type(field_value) is not int:
                        msg=f"intended type is int64  and we got something other than int cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
                        raise Exception(msg)
                elif intended_column_type == np.int32:
                    if field_value is not None and type(field_value) is not int:
                        msg=f"intended type is  int32 and we got something other than int cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
                        raise Exception(msg)
                elif intended_column_type == np.float64:
                    if field_value is not None and type(field_value) is not float:
                        msg=f"intended type is float64  and we got something other than float cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
                        raise Exception(msg)
                elif intended_column_type == np.float32:
                    if field_value is not None and type(field_value) is not float:
                        msg=f"intended type is  32 and we got something other than float cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
                        raise Exception(msg)
                
                # REGARDLESS
                field_value = record[column_name]
                if type(field_value) is np.int64:
                    msg=f"type int64 slipped through cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
                    raise Exception(msg)
                if type(field_value) is np.int32:
                    msg=f"type int32 slipped through cfg:{domain_name} col:{column_name} intended:{intended_column_type} actual:{type(field_value)} val:{field_value}"
                    raise Exception(msg)
    else:
        msg=f"no bueno {domain_name} {domain_name} {table_name}"
        raise Exception(msg)

def convert_and_write(ctx, domain_name, dict_of_lists_by_domain, spark_ds):
    """
        ctx
        name - config_name
        dict_of_lists - dict of record lists keyed by config_name
        spark_ds - destination
    """
    if domain_name in dict_of_lists_by_domain:
        schema = domain_dataset_schema[domain_name]
        domain_list = dict_of_lists_by_domain[domain_name]
        if domain_list:
            correct_types_in_record_list(domain_name, domain_list)
            spark_dff = ctx.spark_session.createDataFrame(domain_list, schema) 
            spark_ds.write_dataframe(spark_dff)
        #if config_list is None or schema is None:
        #    msg=f"NULLNess going on {name}  {schema} {config_list}"
        #    raise Exception(msg)


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
    processing_status = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/processing_stastus"),

# Cedars Sinai
    xml_files=Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_cedars_response_files"),
# Manifest Medex
   # xml_files=Input("ri.foundry.main.dataset.ca873ab5-748b-4f53-9ae4-0c819c7fa3d4"),
   metadata = Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata"),

# March 17th
   # xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
   # metadata = Input("ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79"),

    visit_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
    codemap_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk"),
    valueset_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset")
)
def compute(
    ctx,
    care_site, condition_occurrence,  drug_exposure, location, 
    measurement, observation, person,
    procedure_occurrence, provider, visit_occurrence, processing_status,
    xml_files,  metadata, visit_xwalk_ds, codemap_xwalk_ds, valueset_xwalk_ds ):

    FILE_LIMIT=0
        
    global codemap_xwalk
    global ccda_value_set_mapping_table_dataset
    global visit_concept_xwalk_mapping_dataset
    codemap_xwalk = codemap_xwalk_ds
    ccda_value_set_mapping_table_dataset = valueset_xwalk_ds
    visit_concept_xwalk_mapping_dataset = visit_xwalk_ds

    status_record_list = []
    filestatus_list = list(xml_files.filesystem().ls())
    file_count=0
    omop_dict = {}
    accumulating_domain_rows_dict = {} 
    fs = xml_files.filesystem()
    doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)
    for status in filestatus_list:
        with fs.open(status.path, 'rb') as f:
            br = io.BufferedReader(f)
            tw = io.TextIOWrapper(br) 
            contents = tw.readline()
            for line in tw:
                contents += line
            # Basically selecting content between ClincalDocument tags, looping in case > 1
            for match in doc_regex.finditer(contents):            
                match_tuple = match.groups(0)
                # get data
                new_dict = layer_datasets.process_string_to_dict(match_tuple[0], status.path, False )

                for config_key in new_dict:
                    domain_key = ddl.config_to_domain_name_dict[config_key]
                    if domain_key not in omop_dict:
                        omop_dict[domain_key] = []
                    if config_key in new_dict and new_dict[config_key] is not None:
                        omop_dict[domain_key].extend(new_dict[config_key])

                    # Build a status row for this file and domain
                    if domain_key not in accumulating_domain_rows_dict:
                        if domain_key in new_dict and new_dict[domain_key] is not None:
                            accumulating_domain_rows_dict[domain_key] = len(new_dict[domain_key])
                        else:
                            accumulating_domain_rows_dict[domain_key] = -1
                    else:
                        if domain_key in new_dict and new_dict[domain_key] is not None:
                            accumulating_domain_rows_dict[domain_key] += len(new_dict[domain_key])
                    file_length = -1
                    if match_tuple[0] is not None:
                        file_length = len(match_tuple[0])
                    num_rows = -1
                    if domain_key in new_dict and new_dict[domain_key] is not None:
                        num_new_rows = len(new_dict[domain_key])
                    if domain_key in omop_dict and omop_dict[domain_key] is not None:
                        num_omop_rows = len(omop_dict[domain_key])
                    status_record = {'filename': status.path, 
                                     'file_length': file_length,
                                     'domain': domain_key,
                                     'config':config_key,
                                     'num_new_rows': num_new_rows,
                                     'accumulating_domain_rows': accumulating_domain_rows_dict[domain_key],
                                     'actual_domain_rows': num_omop_rows
                                     }
                    status_record_list.append(status_record)

        file_count += 1
        if FILE_LIMIT > 0 and file_count > FILE_LIMIT:
            break

    spark_dff = ctx.spark_session.createDataFrame(status_record_list) 
    processing_status.write_dataframe(spark_dff)

    convert_and_write(ctx, 'Condition',   omop_dict, condition_occurrence)
    convert_and_write(ctx, 'Care_Site',   omop_dict, care_site)
    convert_and_write(ctx, 'Drug',        omop_dict, drug_exposure)
    convert_and_write(ctx, 'Location',    omop_dict, location)
    convert_and_write(ctx, 'Measurement', omop_dict, measurement)
    convert_and_write(ctx, 'Observation', omop_dict, observation)
    convert_and_write(ctx, 'Person',      omop_dict, person)
    convert_and_write(ctx, 'Procedure',   omop_dict, procedure_occurrence)
    convert_and_write(ctx, 'Provider',    omop_dict, provider)
    convert_and_write(ctx, 'Visit',       omop_dict, visit_occurrence)




    
