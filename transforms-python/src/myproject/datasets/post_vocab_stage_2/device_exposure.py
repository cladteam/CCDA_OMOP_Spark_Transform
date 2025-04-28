from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/device_exposure"),
    devices = Input("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/device_exposure"),
    codemap=Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk")
)
def compute(devices, codemap):
    split_source_value = F.split(devices.device_source_value, '\\|') # splits on a regex, escape the 'or'
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

    df = df.select([
        'device_exposure_id',             #s
        'device_exposure_start_date',     #s
        'device_exposure_start_datetime', #s
        'device_exposure_end_date',       #s
        'device_exposure_end_datetime',   #s
        'unique_device_id',               #s
        'device_type_concept_id',         #s
        'quantity',                       #s
        'provider_id',                    #s
        'visit_detail_id',                #s
        'device_source_value',            #s
##        'device_concept_system', 
##        'device_status', 
        'device_concept_id',               #s
        'device_source_concept_id',        #s  
        'person_id',                       #s
        'visit_occurrence_id',             #s
        'filename']) 

    return df
