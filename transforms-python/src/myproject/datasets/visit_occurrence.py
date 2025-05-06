from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from ..util import ds_schema
from pyspark.sql import types as T

@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/visit_occurrence"),
    omop_eav_dict = Input("ri.foundry.main.dataset.ce6307e8-388a-4c71-b407-4744bee5ec7f"),
)
def compute(ctx, omop_eav_dict):
    df = omop_eav_dict.select('key_value', 'field_name', 'field_value') \
                .where(F.column('domain_name') == 'Visit') \
                .distinct() \
                .groupBy('key_value') \
                .pivot("field_name") \
                .agg(F.first('field_value')) \
                .drop('key_value')

    df = df \
        .withColumn('person_id', df['person_id'].cast(T.LongType())) \
        .withColumn('visit_occurrence_id', df['visit_occurrence_id'].cast(T.LongType())) \
        .withColumn('visit_source_concept_id', df['visit_source_concept_id'].cast(T.IntegerType())) \
        .withColumn('preceding_visit_occurrence_id', df['preceding_visit_occurrence_id'].cast(T.LongType())) \
        .withColumn('discharge_to_concept_id', df['discharge_to_concept_id'].cast(T.IntegerType())) \
        .withColumn('admitting_source_concept_id', df['admitting_source_concept_id'].cast(T.IntegerType())) \
        .withColumn('care_site_id', df['care_site_id'].cast(T.LongType())) \
        .withColumn('provider_id', df['provider_id'].cast(T.LongType())) \
        .withColumn('visit_concept_id', df['visit_concept_id'].cast(T.IntegerType())) \
        .withColumn('visit_start_date',  F.to_date(F.col('visit_start_date'))) \
        .withColumn('visit_start_datetime',  F.to_timestamp(F.col('visit_start_datetime'))) \
        .withColumn('visit_end_date',  F.to_date(F.col('visit_end_date'))) \
        .withColumn('visit_end_datetime',  F.to_timestamp(F.col('visit_end_datetime'))) \
        .withColumn('visit_type_concept_id', df['visit_type_concept_id'].cast(T.IntegerType())) 

    df = df.select([
        'visit_source_value', 'person_id', 'visit_occurrence_id', 'visit_source_concept_id',
        'preceding_visit_occurrence_id', 'discharge_to_concept_id', 'admitting_source_concept_id',
        'care_site_id', 'provider_id', 'visit_concept_id', 'visit_start_date', 'visit_start_datetime',
        'visit_end_date', 'visit_end_datetime', 'visit_type_concept_id', 'admitting_source_value',
        'discharge_to_source_value',
        'filename'
    ])

    df = ctx.spark_session.createDataFrame(df.rdd, ds_schema.domain_dataset_schema['Visit'])

    return(df)

  