from pyspark.sql import functions as F
from pyspark.sql import types as T
from transforms.api import transform_df, Input, Output
from ..util import ds_schema
from . import OMOP_EAV_DICT_FULL_PATH, OUTPUT_FULL_BASE_PATH

@transform_df(
    Output(f"{OUTPUT_FULL_BASE_PATH}/provider"),
    omop_eav_dict=Input(OMOP_EAV_DICT_FULL_PATH),
)
def compute(ctx, omop_eav_dict):
                
    df = omop_eav_dict.select('key_value', 'field_name', 'field_value') \
        .where(F.column('domain_name') == 'Provider') \
        .distinct() \
        .groupBy('key_value') \
        .pivot("field_name") \
        .agg(F.first('field_value')) \
        .drop('key_value')

    df = df \
        .withColumn('year_of_birth', df['year_of_birth'].cast(T.IntegerType())) \
        .withColumn('care_site_id', df['care_site_id'].cast(T.LongType())) \
        .withColumn('provider_id', df['provider_id'].cast(T.LongType())) \
        .withColumn('specialty_concept_id', df['specialty_concept_id'].cast(T.IntegerType())) \
        .withColumn('gender_concept_id', df['gender_concept_id'].cast(T.IntegerType())) \
        .withColumn('specialty_source_concept_id', df['specialty_source_concept_id'].cast(T.IntegerType())) \
        .withColumn('gender_source_concept_id', df['gender_source_concept_id'].cast(T.IntegerType())) 

    df = df.select([
        'year_of_birth', 'care_site_id', 'provider_id', 'provider_name',
        'npi', 'dea', 'specialty_concept_id', 'gender_concept_id',
        'provider_source_value', 'specialty_source_value', 'specialty_source_concept_id',
        'gender_source_value', 'gender_source_concept_id',
        'filename', 'cfg_name'
    ])

    df = ctx.spark_session.createDataFrame(df.rdd, ds_schema.domain_dataset_schema['Provider'])

    return(df)