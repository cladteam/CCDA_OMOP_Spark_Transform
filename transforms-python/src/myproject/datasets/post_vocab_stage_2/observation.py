from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/observation"),
    observations = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/observation"),
    codemap = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk")
)
def compute(observations, codemap):
    split_source_value = F.split(observations.observation_source_value, '\\|') # splits on a regex, escape the 'or'
    df = observations.withColumn('observation_concept_source_system', split_source_value.getItem(1)) \
                     .withColumn('observation_concept_source_code', split_source_value.getItem(0))

    df = df.alias('o') \
           .join(codemap.alias('cm'), \
                 (df.observation_concept_source_system == codemap.src_vocab_code_system) & \
                 (df.observation_concept_source_code == codemap.src_code),\
                 "leftouter") \
           .select('o.*', 'cm.target_concept_id', 'cm.target_domain_id', 'cm.source_concept_id') 

    df = df.withColumn('observation_concept_id', df.target_concept_id)
    df = df.withColumn('observation_source_concept_id', df.source_concept_id)

    df = df.drop('observation_concept_source_system')
    df = df.drop('observation_concept_source_code')

    df = df.select([
        'visit_detail_id', 'visit_occurrence_id', 'provider_id', 'value_as_number', 'person_id', 
        'observation_id', 'observation_concept_id', 'observation_source_value',
        'observation_source_concept_id', 'observation_date', 'observation_datetime',
        'observation_type_concept_id', 'value_as_string', 'value_as_concept_id',
        'qualifier_concept_id', 'qualifier_source_value', 'unit_concept_id',
        'unit_source_value',
        'filename'
    ])

    return df
