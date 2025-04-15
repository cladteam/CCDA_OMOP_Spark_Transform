from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from ..util import ds_schema
from pyspark.sql import types as T

@transform_df(
    Output("ri.foundry.main.dataset.e34c8928-d1c1-4b4e-8026-e6024e6afdbb"),
    omop_eav_dict = Input("ri.foundry.main.dataset.7510d9f2-9597-477c-8f03-e290d81d8d23"),
)
def compute(ctx, omop_eav_dict):
    df = omop_eav_dict.select('key_value', 'field_name', 'field_value') \
                .where(F.column('domain_name') == 'Condition') \
                .distinct() \
                .groupBy('key_value') \
                .pivot("field_name") \
                .agg(F.first('field_value')) \
                .drop('key_value')

    df = df \
        .withColumn('condition_occurrence_id', df['condition_occurrence_id'].cast(T.LongType())) \
        .withColumn('person_id', df['person_id'].cast(T.LongType())) \
        .withColumn('condition_concept_id', df['condition_concept_id'].cast(T.IntegerType())) \
        .withColumn('condition_start_date',  F.to_date(F.col('condition_start_date'))) \
        .withColumn('condition_start_datetime',  F.to_timestamp(F.col('condition_start_datetime'))) \
        .withColumn('condition_end_date',  F.to_date(F.col('condition_end_date'))) \
        .withColumn('condition_end_datetime',  F.to_timestamp(F.col('condition_end_datetime'))) \
        .withColumn('condition_type_concept_id', df['condition_type_concept_id'].cast(T.IntegerType())) \
        .withColumn('condition_status_concept_id', df['condition_status_concept_id'].cast(T.IntegerType())) \
        .withColumn('provider_id', df['provider_id'].cast(T.LongType())) \
        .withColumn('visit_occurrence_id', df['visit_occurrence_id'].cast(T.LongType())) \
        .withColumn('visit_detail_id', df['visit_detail_id'].cast(T.LongType())) \
        .withColumn('condition_source_concept_id', df['condition_source_concept_id'].cast(T.IntegerType()))
        # condition_source_value
        # condition_status_source_value
        # stop_reason


    df = df.select([
        'condition_occurrence_id', 'person_id', 'condition_concept_id', 'condition_start_date',
        'condition_start_datetime', 'condition_end_date', 'condition_end_datetime',
        'condition_type_concept_id', 'condition_status_concept_id', 'stop_reason',
        'provider_id', 'visit_occurrence_id', 'visit_detail_id',
        'condition_source_value', 'condition_source_concept_id', 'condition_status_source_value'
    ])
        

    df = ctx.spark_session.createDataFrame(df.rdd, ds_schema.domain_dataset_schema['Condition'])
    return(df)

