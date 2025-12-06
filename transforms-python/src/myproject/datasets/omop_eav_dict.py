from transforms.api import Input, Output, transform, configure, incremental
from pyspark.sql import types as T
from pyspark.sql import Row
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


from . import OMOP_EAV_DICT_FULL_PATH
from . import OMOP_EAV_DICT_RECORD_FULL_PATH


STEP_SIZE=1000

record_schema = T.StructType( [
        T.StructField("path", T.StringType(), True),
        T.StructField("size", T.LongType(), True),
        T.StructField("modified", T.LongType(), True),
])

@incremental(semantic_version=1, snapshot_inputs=["input_files", "visit_xwalk_ds", "valueset_xwalk_ds", "codemap_xwalk_ds"])
@configure(profile=["DRIVER_MEMORY_EXTRA_LARGE", "EXECUTOR_MEMORY_LARGE", "NUM_EXECUTORS_64"] )
@transform(
    omop_eav_dict=Output(OMOP_EAV_DICT_FULL_PATH),
    previous_files=Output(OMOP_EAV_DICT_RECORD_FULL_PATH),
    input_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
    visit_xwalk_ds=Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset" ),
    valueset_xwalk_ds=Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset" ),
    codemap_xwalk_ds=Input("ri.foundry.main.dataset.28fe6af8-0b22-4b45-86e2-b394c62dcd09" ),
)
def compute(
    ctx,
    omop_eav_dict,
    previous_files,
    input_files,
    visit_xwalk_ds,
    valueset_xwalk_ds,
    codemap_xwalk_ds,
):
    # Killswitch
    #    if not ctx.is_incremental:
    #        omop_eav_dict.abort()
    #        previous_files.abort()
    #        raise Exception("not incremental build, self destructing")
    #        return

    omop_eav_dict.set_mode("modify")

    codemap_dict = get_codemap_dict_list(codemap_xwalk_ds)
    valueset_dict = get_valueset_dict_list(valueset_xwalk_ds)
    visit_map_dict = get_visitmap_dict_list(visit_xwalk_ds)
    if len(codemap_dict) < 1 or len(valueset_dict) < 1 or  len(visit_map_dict) < 1:
        raise Exception(f" DEBUG codemap codemap:{codemap_xwalk_ds.dataframe().count()} {len(codemap_dict)}"
                                     f" valueset:{valueset_xwalk_ds.dataframe().count()} {len(valueset_dict)} "
                                     f" visitmap:{visit_xwalk_ds.dataframe().count()} {len(visit_map_dict)}")


    doc_regex = re.compile(r"(<ClinicalDocument.*?</ClinicalDocument>)", re.DOTALL)
    input_fs = input_files.filesystem()

    codemap_broadcast = ctx.spark_session.sparkContext.broadcast(codemap_dict)
    visitmap_broadcast = ctx.spark_session.sparkContext.broadcast(visit_map_dict)
    valuemap_broadcast = ctx.spark_session.sparkContext.broadcast(valueset_dict)

    def process_file(file_row):
        if True:  # Show that the maps are here inside process_file and work.
            # CODEMAP returns integers
            VT.set_codemap_dict(codemap_broadcast.value)
            try:
                test_value = VT.codemap_xwalk_concept_id({ "vocabulary_oid": "2.16.840.1.113883.6.96", "concept_code": "608837004", "default": 0, } )  # dict not initialized!!
            except KeyError as e:
                msg = f"key error in codemap   {len(VT.get_codemap_xwalk_dict())} "
                raise Exception(msg)
            if ( test_value is None or test_value == "XXX" or test_value == "None" or test_value == 0 ):
                raise Exception("codemap_xwalk test failed with some form of None")
            elif test_value != 1340204:
                msg = f"codemap_xwalk test failed to deliver correct code, got: {test_value}"
                raise Exception(msg)

            try:
                test_value = codemap_broadcast.value[ ("2.16.840.1.113883.6.96", "608837004") ]
            except KeyError as e:
                msg = f"key error in codemap   {len(VT.get_codemap_xwalk_dict())} "
                raise Exception(msg)
            if ( test_value is None or len(test_value) < 1 or test_value[0] is None
                or test_value[0]["target_concept_id"] is None
                or test_value[0]["target_concept_id"] == 0 ):
                raise Exception("codemap_xwalk test failed with some form of None")
            elif test_value[0]["target_concept_id"] != 1340204:
                msg = f"codemap_xwalk test failed to deliver correct code, got: {test_value}"
                raise Exception(msg)

            # VALUESET MAP returns strings
            try:
                test_value = valuemap_broadcast.value[ ("2.16.840.1.113883.6.238", "2106-3") ]
            except KeyError as e:
                msg = f"valueset 1: key error in valueset map   {len(VT.get_codemap_xwalk_dict())} "
                raise Exception(msg)
            if (test_value is None or len(test_value) < 1 or test_value[0] is None
                or test_value[0]["target_concept_id"] is None
                or test_value[0]["target_concept_id"] == 0 ):
                raise Exception( "valueset 1: valueset_xwalk test failed with some form of None" )
            elif test_value[0]["target_concept_id"] != "8527":
                msg = f"valueset 1: valueset_xwalk test failed to deliver correct code, got: {test_value}"
                raise Exception(msg)

            # VALUESET MAP returns strings: MALE
            try:
                test_value = valuemap_broadcast.value[ ("2.16.840.1.113883.5.1", "M") ]
            except KeyError as e:
                msg = f"valueset 1: key error in valueset map   {len(VT.get_codemap_xwalk_dict())} "
                raise Exception(msg)
            if (test_value is None or len(test_value) < 1 or test_value[0] is None
                or test_value[0]["target_concept_id"] is None
                or test_value[0]["target_concept_id"] == 0 ):
                raise Exception( "valueset 1: valueset_xwalk test failed with some form of None" )
            elif test_value[0]["target_concept_id"] != "8507":
                msg = f"valueset M: valueset_xwalk test failed to deliver correct code, got: {test_value}"
                raise Exception(msg)
           # VALUESET MAP returns strings : FEMALE
            try:
                test_value = valuemap_broadcast.value[ ("2.16.840.1.113883.5.1", "F") ]
            except KeyError as e:
                msg = f"valueset 1: key error in valueset map   {len(VT.get_codemap_xwalk_dict())} "
                raise Exception(msg)
            if (test_value is None or len(test_value) < 1 or test_value[0] is None
                or test_value[0]["target_concept_id"] is None
                or test_value[0]["target_concept_id"] == 0 ):
                raise Exception( "valueset 1: valueset_xwalk test failed with some form of None" )
            elif test_value[0]["target_concept_id"] != "8532":
                msg = f"valueset F: valueset_xwalk test failed to deliver correct code, got: {test_value}"
                raise Exception(msg)

            # VISIT SET MAP returns strings
            try:
                test_value = visitmap_broadcast.value[ ("2.16.840.1.113883.6.259", "1026-4") ]
            except KeyError as e:
                msg = f"valueset 1: key error in visit map.  {len(VT.get_codemap_xwalk_dict())} "
                raise Exception(msg)
            if ( test_value is None or len(test_value) < 1 or test_value[0] is None
                or test_value[0]["target_concept_id"] is None
                or test_value[0]["target_concept_id"] == 0 ):
                raise Exception("visit set_xwalk test failed with some form of None")
            elif test_value[0]["target_concept_id"] != "9201":
                msg = ( f"visitmap  test failed to deliver correct code, got: {test_value}" )
                raise Exception(msg)

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

    # Exclude what we've already copied, and get a subset/batch
    previous_files_df = previous_files.dataframe(schema=record_schema, mode="previous")
    input_files_df = input_fs.files()
    input_files_df = input_files_df.join(previous_files_df, ["path"], "leftanti")
    input_files_df = input_files_df.limit(STEP_SIZE).checkpoint(eager=True)

    # Fan-out over the cluster to process files
    rdd = input_files_df.rdd.flatMap(process_file)

    processed_df = rdd.toDF(omop_dict_schema)
    omop_eav_dict.write_dataframe(processed_df)

    # Append rows to record of previous files
    previous_files.write_dataframe(input_files_df)
