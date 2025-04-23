from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/measurement"),
    measurements = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/measurement"),
    codemap = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk")
)
def compute(measurements, codemap):
    split_source_value = F.split(measurements.measurement_source_value, '\\|') # splits on a regex, escape the 'or'
    df = measurements.withColumn('measurement_concept_source_system', split_source_value.getItem(1)) \
                     .withColumn('measurement_concept_source_code', split_source_value.getItem(0))

    df = df.alias('m') \
           .join(codemap.alias('cm'), \
                 (df.measurement_concept_source_system == codemap.src_vocab_code_system) & \
                 (df.measurement_concept_source_code == codemap.src_code),
                 "leftouter") \
           .select('m.*', 'cm.target_concept_id', 'cm.target_domain_id', 'cm.source_concept_id') 

        #    .select('m.*', 'cm.target_concept_id', 'cm.target_domain_id', 'cm.source_concept_id') 
        #    .select(df['*'], codemap['target_concept_id', 'target_domain_id', 'source_concept_id'])

    df = df.withColumn('measurement_concept_id', df.target_concept_id)
    df = df.withColumn('measurement_source_concept_id', df.source_concept_id)

   ## df = df.withColumn('measurement_domain_id', df.target_domain_id)

    df = df.drop('measurement_concept_source_system')
    df = df.drop('measurement_concept_source_code')

    df = df.select([
    'visit_detail_id', 'visit_occurrence_id', 'provider_id', 'value_as_number', 'person_id', 
    'measurement_id', 'measurement_concept_id', 'measurement_source_value',
    'measurement_source_concept_id', 'measurement_date',  'measurement_datetime', 'measurement_time',
    'measurement_type_concept_id', 
    'range_high', 'range_low',
    ##'value_as_string', 
    'value_as_concept_id',
    'operator_concept_id',
    #'qualifier_concept_id', 'qualifier_source_value', 
    'unit_concept_id',
    'unit_source_value',
    'value_source_value',
    # "measurement_concept_name": T.StringType(),
    # "measurement_type_concept_name": T.StringType(),
    # "operator_concept_name": T.StringType(),
    # "value_as_concept_name": T.StringType(),
    # "unit_concept_name": T.StringType(),
    #"measurement_source_concept_name": T.StringType()
    'filename'
    ])

    ### MISSING BIG SELECT

    return df
