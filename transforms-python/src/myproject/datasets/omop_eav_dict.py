from transforms.api import Input, Output, transform, configure, incremental
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

STEP_SIZE=1000
""" All In
    has batches
    has broadcast variables, but not for codemap (too big)
    has flatmp
"""   


record_schema = T.StructType([
    T.StructField('path', T.StringType(), True),
    T.StructField('size', T.LongType(), True),
    T.StructField('modified', T.LongType(), True)
])


@incremental(semantic_version=1, snapshot_inputs=["input_files"] )
@configure(profile=['DRIVER_MEMORY_EXTRA_LARGE', 'EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_64'])
@transform(
    omop_eav_dict = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/omop_eav_dict_May3"),
    previous_files = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/omop_eav_dict_May3"),
    input_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
    visit_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
    valueset_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset") )
def compute(ctx,
    omop_eav_dict, previous_files,
    input_files,
    visit_xwalk_ds, valueset_xwalk_ds,  ## codemap_xwalk_ds, 
    ):

    # Killswitch
#    if not ctx.is_incremental:
#        omop_eav_dict.abort()
#        previous_files.abort()
#        raise Exception("not incremental build, self destructing")
#        return

    omop_eav_dict.set_mode("modify")

    ## codemap_dict = get_codemap_dict(codemap_xwalk_ds)
    value_set_map_dict = get_valueset_dict(valueset_xwalk_ds)
    visit_map_dict = get_visit_dict(visit_xwalk_ds)

    doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)
    input_fs = input_files.filesystem()

    ## codemap_broadcast = ctx.spark_session.sparkContext.broadcast(codemap_dict)  # BROADCAST
    visitmap_broadcast = ctx.spark_session.sparkContext.broadcast(value_set_map_dict)  # BROADCAST
    valuemap_broadcast = ctx.spark_session.sparkContext.broadcast(visit_map_dict)  # BROADCAST

    def process_file(file_row):
        with input_fs.open(file_row.path, 'rb') as f:
            contents = f.read().decode('utf-8')

            # Basically selecting content between ClincalDocument tags, looping in case > 1
            for match in doc_regex.finditer(contents):
                match_tuple = match.groups(0)
                xml_content = match_tuple[0]

                new_dict = layer_datasets.process_string_to_dict_no_codemap(\
                   #xml_content, file_status.path, False, \
                   xml_content, file_row.path, False, \
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

    # exclude what we've already copied, and get a subset/batch
    previous_files_df = previous_files.dataframe(schema=record_schema, mode="previous") # now complains about unknown keyword arg for schema, worked yesterday!

    input_files_df = input_fs.files()
    input_files_df = input_files_df.join(previous_files_df, ['path'], 'leftanti')
    input_files_df = input_files_df.limit(STEP_SIZE).checkpoint(eager=True)

    rdd = input_files_df.rdd.flatMap(process_file) # AttributeError: 'list' object has no attribute 'rdd'

    processed_df = rdd.toDF(omop_dict_schema)
###    processed_df = processed_df.distinct() 3x slower, do in individual tables?
    omop_eav_dict.write_dataframe(processed_df)

    # Append rows to record of previous files
    previous_files.write_dataframe(input_files_df)











