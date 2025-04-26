from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentfiedData/OMOP_spark/post_vocab_stage_2/device_exposure"),
    devices = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/device_exposure"),
    codemap = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk")
)
def compute(devices, codemap):
    split_source_value = F.split(devices.condition_source_value, '\\|') # splits on a regex, escape the 'or'
    df = devices.withColumn('device_concept_source_system', split_source_value.getItem(1)) \
                  .withColumn('device_concept_source_code', split_source_value.getItem(0))

    df = df.join(codemap, (df.device_concept_source_system == codemap.src_vocab_code_system) & \
                          (df.device_concept_source_code == codemap.src_code),
                          "leftouter")

    df = df.withColumn('device_source_concept_id', df.source_concept_id)
    df = df.withColumn('device_concept_id', df.target_concept_id)
    df = df.withColumn('device_domain_id', df.target_domain_id)

    df = df.filter(df['device_domain_id'] == 'Device')

    df = df.drop('device_concept_source_system')
    df = df.drop('device_concept_source_code')
    df = df.drop('device_domain_id')

    #### MISSING SELECT ? TODO
    return df
