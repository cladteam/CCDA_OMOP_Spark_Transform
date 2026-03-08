from transforms.api import Input, Output, transform, configure, incremental
from pyspark.sql import types as T
from pyspark.sql import Row, SparkSession, DataFrame
import re

from ..util.correct_types import correct_types_in_record_list
from prototype_2 import layer_datasets
from prototype_2 import ddl
from prototype_2 import value_transformations as VT
from ..util.omop_eav_dict_common import omop_dict_schema
from ..util.omop_eav_dict_common import flatten_and_stringify_record_dict
from ..util.omop_eav_dict_common import get_codemap_dict_list
from ..util.omop_eav_dict_common import get_valueset_dict_list
from ..util.omop_eav_dict_common import get_visitmap_dict_list
# from ..util.omop_eav_dict_common import reset_previous_files_listing

from . import OMOP_EAV_DICT_FULL_PATH
from . import OMOP_EAV_DICT_RECORD_FULL_PATH
from . import OMOP_EAV_DICT_STEP_SIZE
from . import DUMMY_TRIGGER_PATH

import logging
 
# Start fresh by rebuilding _dummy_incremental_reset_trigger prior to rebuilding OMOP_EAV_DICT tables
# Run all at once if OMOP_EAV_DICT_STEP_SIZE = None (in __init__.py)

record_schema = T.StructType( [
        T.StructField("path", T.StringType(), True),
        T.StructField("size", T.LongType(), True),
        T.StructField("modified", T.LongType(), True),
])
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

@incremental(semantic_version=1, snapshot_inputs=[
    "input_files", 
    "visit_xwalk_ds", 
    "valueset_xwalk_ds", 
    "codemap_xwalk_ds", 
    "ccda_metadata_ds", 
    "partner_mapping_ds"])
@configure(profile=["DRIVER_MEMORY_EXTRA_LARGE", "EXECUTOR_MEMORY_LARGE", "NUM_EXECUTORS_64"] )
@transform(
    omop_eav_dict=Output(OMOP_EAV_DICT_FULL_PATH),
    previous_files=Output(OMOP_EAV_DICT_RECORD_FULL_PATH),
    input_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
    visit_xwalk_ds=Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
    valueset_xwalk_ds=Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset"),
    codemap_xwalk_ds=Input("ri.foundry.main.dataset.2f6c08ad-6616-404c-b26c-dbd2049290b6"),
    dummy_incremental_reset_trigger=Input(DUMMY_TRIGGER_PATH),
    ccda_metadata_ds=Input("ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79"),
    partner_mapping_ds=Input("/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id")
)
def compute(
    ctx,
    omop_eav_dict,
    previous_files,
    input_files,
    visit_xwalk_ds,
    valueset_xwalk_ds,
    codemap_xwalk_ds,
    dummy_incremental_reset_trigger,
    ccda_metadata_ds,
    partner_mapping_ds
):
    logger.setLevel(logging.WARNING)
    omop_eav_dict.set_mode("modify")

    codemap_dict = get_codemap_dict_list(codemap_xwalk_ds)
    valueset_dict = get_valueset_dict_list(valueset_xwalk_ds)
    visit_map_dict = get_visitmap_dict_list(visit_xwalk_ds)

    # New Mapping Logic for MSPI and Partner ID
    metadata_df = ccda_metadata_ds.dataframe().select("response_file_path", "mspi", "healthcare_site")
    partner_df = partner_mapping_ds.dataframe().select("healthcare_site", "data_partner_id")

    # Join to get a unified reference dataframe
    mapping_ref_df = metadata_df.join(partner_df, "healthcare_site").collect()

    # Create the dictionaries
    mspi_map: dict = {row.response_file_path: row.mspi for row in mapping_ref_df}
    partner_map = {row.response_file_path: row.data_partner_id for row in mapping_ref_df}

    if len(codemap_dict) < 1 or len(valueset_dict) < 1 or len(visit_map_dict) < 1:
        raise Exception(f" DEBUG codemap codemap:{codemap_xwalk_ds.dataframe().count()} {len(codemap_dict)}"
                                     f" valueset:{valueset_xwalk_ds.dataframe().count()} {len(valueset_dict)} "
                                     f" visitmap:{visit_xwalk_ds.dataframe().count()} {len(visit_map_dict)}")
    
    metadata_df = ccda_metadata_ds.dataframe().select("response_file_path", "healthcare_site")
    # partner_df = partner_mapping_ds.dataframe().select("healthcare_site", "data_partner_id")
    # filename_to_partner_df = metadata_df.join(partner_df, "healthcare_site").select("response_file_path", "data_partner_id")

    doc_regex = re.compile(r"(<ClinicalDocument.*?</ClinicalDocument>)", re.DOTALL)
    input_fs = input_files.filesystem()

    codemap_broadcast = ctx.spark_session.sparkContext.broadcast(codemap_dict)
    visitmap_broadcast = ctx.spark_session.sparkContext.broadcast(visit_map_dict)
    valuemap_broadcast = ctx.spark_session.sparkContext.broadcast(valueset_dict)
    mspi_broadcast = ctx.spark_session.sparkContext.broadcast(mspi_map)
    partner_broadcast = ctx.spark_session.sparkContext.broadcast(partner_map)

    def process_file(file_row):
        VT.set_mspi_map(mspi_broadcast.value)       # Crucial for DERIVED person_id
        VT.set_partner_map(partner_broadcast.value) # Crucial for data_partner_id
     

        with input_fs.open(file_row.path, "rb") as f:
            contents = f.read().decode("utf-8")
            # Basically selecting content between ClincalDocument tags, looping in case > 1
            for match in doc_regex.finditer(contents):
                match_tuple = match.groups(0)
                xml_content = match_tuple[0]

                new_dict = layer_datasets.process_string_to_dict(
                    xml_content,
                    file_row.path,
                    False,
                    codemap_broadcast.value,
                    visitmap_broadcast.value,
                    valuemap_broadcast.value,
                    mspi_broadcast.value,  
                    partner_broadcast.value
                )

                # Convert list of dicts to yield of Rows.
                for config_name in new_dict.keys():
                    if new_dict[config_name] is not None:
                        domain_name = ddl.config_to_domain_name_dict[config_name]
                        correct_types_in_record_list(domain_name, new_dict[config_name])
                        for record_dict in new_dict[config_name]:
                            eav_list = flatten_and_stringify_record_dict(
                                domain_name, record_dict
                            )
                            for eav_record in eav_list:
                                yield (Row(**eav_record))

    # If transform is running as an incremental job, remove previously processed files
    if ctx.is_incremental:
        # Remove files from the to-process list that have already been processed
        previous_files_df = previous_files.dataframe(schema=record_schema, mode="previous")
        input_files_df = input_fs.files()
        input_files_df = input_files_df.join(previous_files_df, ["path"], "leftanti")
        # Set appropriate write mode on the target table (modify=append)
        omop_eav_dict.set_mode("modify")
        previous_files.set_mode("modify")
    # If transform is NOT running as an incremental job, do not remove any files
    else:
        # Same pattern as above, but replace previous_files df with an empty df
        spark: SparkSession = ctx.spark_session
        empty_df: DataFrame = spark.createDataFrame([], schema=record_schema)
        previous_files_df = empty_df
        input_files_df = input_fs.files()
        input_files_df = input_files_df.join(previous_files_df, ["path"], "leftanti")
        # Set appropriate write mode on the target table (replace=fresh start)
        # typically triggered by a build of the dummy_incremental_reset_trigger,
        # which can set ctx.is_incremental to false
        omop_eav_dict.set_mode("replace")
        previous_files.set_mode("replace")

    # Process in batches if a batch size is passed
    if OMOP_EAV_DICT_STEP_SIZE:
        input_files_df = input_files_df.limit(OMOP_EAV_DICT_STEP_SIZE).checkpoint(eager=True)

    # Fan-out over the cluster to process files
    rdd = input_files_df.rdd.flatMap(process_file)

    processed_df = rdd.toDF(omop_dict_schema)
    omop_eav_dict.write_dataframe(processed_df)

    # Append or write filenames to the tracking dataset
    # Behavior is determined by the ctx.is_incremental attribute, behind the scenes
    previous_files.write_dataframe(input_files_df)
