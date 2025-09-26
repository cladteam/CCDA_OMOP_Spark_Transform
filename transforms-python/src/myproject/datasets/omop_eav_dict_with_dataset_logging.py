
# This is a modified version of omop_eav_dict.py copied on Sept 12, 2025 for debug purpose. This contains logging messages at every step.
#  The purpose of this file is to log the output from process_file function to a separate dataset to check for the XML parsing.



from transforms.api import Input, Output, transform, configure, incremental
from pyspark.sql import types as T
from pyspark.sql import Row
from pyspark.sql import DataFrame # Import DataFrame for type hinting
from pyspark.sql import functions as F
from pyspark.sql.functions import col, monotonically_increasing_id
import io
import re
import json
import numpy as np
from datetime import datetime, date
import logging
import sys # Import sys to redirect stdout
from ..util.correct_types import correct_types_in_record_list
from ..util.ds_schema import domain_key_fields
from prototype_2 import layer_datasets
from prototype_2.domain_dataframe_column_types import domain_dataframe_column_types
from prototype_2 import ddl
from ..util.omop_eav_dict_common import omop_dict_schema
from ..util.omop_eav_dict_common import concat_key
from ..util.omop_eav_dict_common import lookup_key_value
from ..util.omop_eav_dict_common import flatten_and_stringify_record_dict
from ..util.omop_eav_dict_common import get_valueset_dict_list
from ..util.omop_eav_dict_common import get_visit_dict_list


STEP_SIZE=3


# --- DEBUGGING CONFIGURATION ---
# These variables must be defined at the top level
ENABLE_DEBUG_LOGS = True
DEBUG_FILE_LIMIT = 3
FORCE_FULL_RUN = True
# -------------------------------


# The logger must be defined at the top level
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


record_schema = T.StructType([
T.StructField('path', T.StringType(), True),
T.StructField('size', T.LongType(), True),
T.StructField('modified', T.LongType(), True)
])


# Define the schema for the new log output
# This is a temporary combined schema for the RDD before splitting
combined_schema = T.StructType([
T.StructField('type', T.StringType(), True),
T.StructField('timestamp', T.TimestampType(), True),
T.StructField('log_level', T.StringType(), True),
T.StructField('file_path', T.StringType(), True),
T.StructField('message', T.StringType(), True),
T.StructField('person_id', T.StringType(), True),
T.StructField('source_id', T.StringType(), True),
T.StructField('domain_name', T.StringType(), True),
T.StructField('key_type', T.StringType(), True),
T.StructField('key_value', T.StringType(), True),
T.StructField('field_name', T.StringType(), True),
T.StructField('field_value', T.StringType(), True),
])


# Correct schema for the final logs DataFrame
log_schema = T.StructType([
T.StructField('timestamp', T.TimestampType(), True),
T.StructField('log_level', T.StringType(), True),
T.StructField('file_path', T.StringType(), True),
T.StructField('message', T.StringType(), True),
])




@incremental(semantic_version=1, snapshot_inputs=["input_files","visit_xwalk_ds", "valueset_xwalk_ds"] )
@configure(profile=['DRIVER_MEMORY_EXTRA_LARGE', 'EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_8'])
@transform(
   omop_eav_dict = Output("/All of Us-cdb223/HIN - HIE/CCDA/scratch/Lakshmi/omop_eav_dict_LA1"),
   process_file_logs = Output("/All of Us-cdb223/HIN - HIE/CCDA/scratch/Lakshmi/process_file_log_LA1"),
   input_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
   visit_xwalk_ds = Input("ri.foundry.main.dataset.832f8106-d7ee-4364-9082-d48c49302dd8"),
   valueset_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset") )
def compute(ctx,
   omop_eav_dict,
   process_file_logs,
   input_files,
   visit_xwalk_ds, valueset_xwalk_ds,
):


   logger.warning("\n--- DEBUG: Starting the compute function ---")


   omop_eav_dict.set_mode("modify")


   if ENABLE_DEBUG_LOGS:
       valueset_df = valueset_xwalk_ds.dataframe()
       row_count = valueset_df.count()
       logger.warning(f"--- DEBUG: valueset_xwalk_ds has {row_count} rows. ---")
       if row_count == 0:
           logger.warning("--- DEBUG: valueset_xwalk_ds is empty! ---")
       else:
           logger.warning("--- DEBUG: valueset_xwalk_ds sample (first 5 rows): ---")
           valueset_df.show(5, truncate=False)


   if ENABLE_DEBUG_LOGS:
       visit_df = visit_xwalk_ds.dataframe()
       row_count = visit_df.count()
       logger.warning(f"--- DEBUG: visit_xwalk_ds has {row_count} rows. ---")
       if row_count == 0:
           logger.warning("--- DEBUG: visit_xwalk_ds is empty! ---")
       else:
           logger.warning("--- DEBUG: visit_xwalk_ds sample (first 5 rows): ---")
           visit_df.show(5, truncate=False)


   value_set_map_dict = get_valueset_dict_list(valueset_xwalk_ds)
   logger.warning(f"--- DEBUG: value_set_map_dict data:{value_set_map_dict}. ---")
   visit_map_dict = get_visit_dict_list(visit_xwalk_ds)


   doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)
   input_fs = input_files.filesystem()


   visitmap_broadcast = ctx.spark_session.sparkContext.broadcast(visit_map_dict)
   valuemap_broadcast = ctx.spark_session.sparkContext.broadcast(value_set_map_dict)


   if ENABLE_DEBUG_LOGS:
       logger.warning(f"--- DEBUG: Broadcasted Value Set Map has {len(valuemap_broadcast.value)} top-level keys. ---")
       logger.warning(f"--- DEBUG: Broadcasted Visit Map has {len(visitmap_broadcast.value)} keys. ---")


   def process_file(file_row):
       file_path = file_row.path


       # Generator for log rows - now includes 'type' field
       def log(level, message):
           return Row(type="log", timestamp=datetime.now(), log_level=level, file_path=file_path, message=message)


       # This log line should always be written, regardless of DEBUG flag.
       yield log("INFO", "Testing log output")


       if ENABLE_DEBUG_LOGS:
           yield log("INFO", f"Starting to process file: {file_path}")
           if not visitmap_broadcast.value:
               yield log("WARNING", f"File {file_path}: The visit map broadcast is empty!")
           if not valuemap_broadcast.value:
               yield log("WARNING", f"File {file_path}: The value set map broadcast is empty!")


       try:
           if ENABLE_DEBUG_LOGS:
               yield log("INFO", f"Attempting to open file: {file_path}")
           with input_fs.open(file_path, 'rb') as f:
               contents = f.read().decode('utf-8')


           if ENABLE_DEBUG_LOGS:
               yield log("INFO", f"Successfully read contents for file: {file_path}. Content size: {len(contents)} bytes")


           try:
               if ENABLE_DEBUG_LOGS:
                   yield log("INFO", f"Starting regex search for documents in file: {file_path}")
               doc_matches = list(doc_regex.finditer(contents))
               if ENABLE_DEBUG_LOGS:
                   yield log("INFO", f"Found {len(doc_matches)} XML documents in file: {file_path}")


           except Exception as e:
               yield log("ERROR", f"CRITICAL ERROR: Regex failed on file {file_path}. Error: {e}")
               return


           for i, match in enumerate(doc_matches):
               xml_content = match.groups(0)[0]
               if ENABLE_DEBUG_LOGS:
                   yield log("INFO", f"Processing XML document {i+1} of {len(doc_matches)} in file: {file_path}")
                   yield log("INFO", f"xml_content: {xml_content}")
                   yield log("INFO", f"visitmap_broadcast.value , {visitmap_broadcast.value}")
                   yield log("INFO", f"valuemap_broadcast.value , {valuemap_broadcast.value}")


               try:
                   new_dict = layer_datasets.process_string_to_dict_no_codemap(
                       xml_content, file_path, False,
                       visitmap_broadcast.value, valuemap_broadcast.value)
                  
                   yield log("INFO", f"Type of new_dict: {type(new_dict)}")
                   yield log("INFO", f"Content of new_dict: {new_dict}")
                  
                   if ENABLE_DEBUG_LOGS and 'Person' in new_dict and new_dict['Person']:
                      yield log("INFO", f"XML Parser Output for Person: {new_dict['Person'][0]}")


                   if ENABLE_DEBUG_LOGS:
                       yield log("INFO", f"XML parsing for doc {i+1} returned keys: {new_dict.keys()}")
               except Exception as e:
                   yield log("ERROR", f"CRITICAL ERROR: XML parsing failed for document {i+1} in file {file_path}. Error: {e}")
                   continue


               for config_name in new_dict.keys():
                   if new_dict[config_name] is not None:
                       domain_name = ddl.config_to_domain_name_dict[config_name]
                       if ENABLE_DEBUG_LOGS:
                           yield log("INFO", f"Correcting types for domain '{domain_name}' for doc {i+1}. Found {len(new_dict[config_name])} records.")
                           yield log("INFO", f"-----------{domain_name},---------,{new_dict[config_name]}-------------")
                           corrected_records = correct_types_in_record_list(domain_name, new_dict[config_name])
                           yield log("INFO", f"Content of corrected_records: {corrected_records}")
      


                       if corrected_records:
                           for j, record_dict in enumerate(corrected_records):
                               if ENABLE_DEBUG_LOGS and j < 1:
                                   yield log("INFO", f"Record {j+1} of {len(corrected_records)} in domain '{domain_name}' for doc {i+1}: {record_dict}")


                               try:
                                   eav_list = flatten_and_stringify_record_dict(domain_name, record_dict)
                               except Exception as e:
                                   yield log("ERROR", f"CRITICAL ERROR: Flattening failed for record {j+1} in domain '{domain_name}' in file {file_path}. Error: {e}")
                                   continue


                               if ENABLE_DEBUG_LOGS and j < 1:
                                   if eav_list:
                                       yield log("INFO", f"First EAV record after flatten: {eav_list[0]}")
                                   else:
                                       yield log("INFO", "No EAV records were generated from this record dict.")


                               for eav_record in eav_list:
                                   yield Row(type="data", **eav_record) # Tag data rows


       except Exception as e:
           yield log("ERROR", f"--- CRITICAL ERROR: Failed to process file {file_path}. General error: {e}")
           raise


   # Start by getting the files dataframe.
   input_files_df = input_fs.files()
   logger.warning("--- DEBUG: input_files_df has been created. ---")
   logger.warning(f"--- DEBUG1: input files data {input_files_df}. ---")


   input_files_df = input_files_df.limit(STEP_SIZE)
   logger.warning(f"--- DEBUG2: input files data {input_files_df}. ---")


   try:
       file_count = input_files_df.count()
       logger.warning(f"--- DEBUG: Input files to process: {file_count} ---")
   except Exception as e:
       logger.error(f"--- CRITICAL ERROR: Failed to count input files. This is likely an infrastructure or permissions issue with the source dataset. Error: {e}", exc_info=True)
       # Exit gracefully since we can't proceed without a valid file list.
       return


   # Process files and collect both data and logs
   combined_rdd = input_files_df.rdd.flatMap(process_file)


   # Separate data and logs
   data_rdd = combined_rdd.filter(lambda row: getattr(row, 'type', None) == 'data')
   logs_rdd = combined_rdd.filter(lambda row: getattr(row, 'type', None) == 'log')


   logger.warning("--- DEBUG3: Separating RDDs and writing to outputs. ---")


   # Write the logs to a dedicated dataset first
   # This ensures that any errors that occur while writing the data are logged.
   logs_df = logs_rdd.toDF(log_schema)
   process_file_logs.write_dataframe(logs_df)


   # Clean the data RDD and write the main data
   data_df = data_rdd.map(lambda row: Row(**{key: value for key, value in row.asDict().items() if key != "type"}))
   processed_df = data_df.toDF(omop_dict_schema)
   omop_eav_dict.write_dataframe(processed_df)


   logger.warning("--- DEBUG4: Job finished successfully. Check the outputs for data and logs. ---")





