
from transforms.api import transform, Input, Output, configure
from pyspark.sql import types as T


from prototype_2 import set_codemap_xwalk
from prototype_2 import set_ccda_value_set_mapping_table_dataset
from prototype_2 import set_visit_concept_xwalk_mapping_dataset



@transform(
    output_df = Output("/All of Us-cdb223/HIN - HIE/CCDA/scratch/Chris/debug_output"),

    xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
    metadata = Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata"),
    visit_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
    codemap_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk"),
    valueset_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset"),
)
def compute(ctx, output_df,
            visit_xwalk_ds, codemap_xwalk_ds, valueset_xwalk_ds ):

    test_value = codemap_xwalk_ds[ (codemap_xwalk_ds['src_vocab_code_system'] == '2.16.840.1.113883.6.96') \
                              & (codemap_xwalk_ds['src_code']  == '608837004') ]
    if test_value is None or test_value == 'XXX' or test_value == 'None':
        raise Exception("codemap_xwalk test failed with some form of None")
    if test_value != '1340204':
        raise Exception("codemap_xwalk test failed to deliver correct code")


    msg_list = []
    msg_list.append("test 1")
    msg_list.append(test_value)

    schema = T.StructType([
        T.StructField("message", T.StringType(), True)
    ])
    df = ctx.spark_session.createDataFrame(msg_list, schema)
    output_df.write_dataframe(df)

