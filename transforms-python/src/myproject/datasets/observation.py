from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from ..util import ds_schema
from pyspark.sql import types as T

@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/observation"),
    omop_eav_dict = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/omop_eav_dict_May10"),
)
def compute(ctx, omop_eav_dict):
    df = omop_eav_dict.select('key_value', 'field_name', 'field_value') \
                .where(F.column('domain_name') == 'Observation') \
                .distinct() \
                .groupBy('key_value') \
                .pivot("field_name") \
                .agg(F.first('field_value')) \
                .drop('key_value')

    df = df \
        .withColumn('visit_detail_id', df['visit_detail_id'].cast(T.LongType())) \
        .withColumn('visit_occurrence_id', df['visit_occurrence_id'].cast(T.LongType())) \
        .withColumn('provider_id', df['provider_id'].cast(T.LongType())) \
        .withColumn('value_as_number', df['value_as_number'].cast(T.DoubleType())) \
        .withColumn('person_id', df['person_id'].cast(T.LongType())) \
        .withColumn('observation_id', df['observation_id'].cast(T.LongType())) \
        .withColumn('observation_concept_id', df['observation_concept_id'].cast(T.IntegerType())) \
        .withColumn('observation_source_concept_id', df['observation_source_concept_id'].cast(T.IntegerType())) \
        .withColumn('observation_date',  F.to_date(F.col('observation_date'))) \
        .withColumn('observation_datetime',  F.to_timestamp(F.col('observation_datetime'))) \
        .withColumn('observation_type_concept_id', df['observation_type_concept_id'].cast(T.IntegerType())) \
        .withColumn('value_as_concept_id', df['value_as_concept_id'].cast(T.IntegerType())) \
        .withColumn('qualifier_concept_id', df['qualifier_concept_id'].cast(T.IntegerType())) \
        .withColumn('unit_concept_id', df['unit_concept_id'].cast(T.IntegerType()))

    df = df.select([
        'visit_detail_id', 'visit_occurrence_id', 'provider_id', 'value_as_number', 'person_id', 
        'observation_id', 'observation_concept_id', 'observation_source_value',
        'observation_source_concept_id', 'observation_date', 'observation_datetime',
        'observation_type_concept_id', 'value_as_string', 'value_as_concept_id',
        'qualifier_concept_id', 'qualifier_source_value', 'unit_concept_id',
        'unit_source_value',
        'filename'
    ])

    df = ctx.spark_session.createDataFrame(df.rdd, ds_schema.domain_dataset_schema['Observation'])
    return(df)

