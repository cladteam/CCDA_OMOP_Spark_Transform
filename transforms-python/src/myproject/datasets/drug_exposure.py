from pyspark.sql import functions as F
from pyspark.sql import types as T
from transforms.api import transform_df, Input, Output
from ..util import ds_schema
from . import OMOP_EAV_DICT_FULL_PATH, OUTPUT_FULL_BASE_PATH

@transform_df(
    Output(f"{OUTPUT_FULL_BASE_PATH}/drug_exposure"),
    omop_eav_dict=Input(OMOP_EAV_DICT_FULL_PATH),
)
def compute(ctx, omop_eav_dict):
    df = omop_eav_dict.select('key_value', 'field_name', 'field_value') \
                .where(F.column('domain_name') == 'Drug') \
                .distinct() \
                .groupBy('key_value') \
                .pivot("field_name") \
                .agg(F.first('field_value')) \
                .drop('key_value')

    df = df \
        .withColumn('visit_detail_id', df['visit_detail_id'].cast(T.LongType())) \
        .withColumn('verbatim_end_date',  F.to_date(F.col('verbatim_end_date'))) \
        .withColumn('visit_occurrence_id', df['visit_occurrence_id'].cast(T.LongType())) \
        .withColumn('provider_id', df['provider_id'].cast(T.LongType())) \
        .withColumn('days_supply', df['days_supply'].cast(T.IntegerType())) \
        .withColumn('quantity', df['quantity'].cast(T.FloatType())) \
        .withColumn('person_id', df['person_id'].cast(T.LongType())) \
        .withColumn('drug_exposure_id', df['drug_exposure_id'].cast(T.LongType())) \
        .withColumn('drug_concept_id', df['drug_concept_id'].cast(T.IntegerType())) \
        .withColumn('drug_exposure_start_date',  F.to_date(F.col('drug_exposure_start_date'))) \
        .withColumn('drug_exposure_start_datetime',  F.to_timestamp(F.col('drug_exposure_start_datetime'))) \
        .withColumn('drug_exposure_end_date',  F.to_date(F.col('drug_exposure_end_date'))) \
        .withColumn('drug_exposure_end_datetime',  F.to_timestamp(F.col('drug_exposure_end_datetime'))) \
        .withColumn('drug_type_concept_id', df['drug_type_concept_id'].cast(T.IntegerType())) \
        .withColumn('refills', df['refills'].cast(T.IntegerType())) \
        .withColumn('route_concept_id', df['route_concept_id'].cast(T.IntegerType())) \
        .withColumn('drug_source_concept_id', df['drug_source_concept_id'].cast(T.IntegerType())) \

    df = df.select([ 
        'visit_detail_id', 'sig', 'verbatim_end_date',
        'visit_occurrence_id', 'provider_id', 'days_supply', 'quantity',
        'person_id', 'drug_exposure_id', 'drug_concept_id',
        'drug_exposure_start_date', 'drug_exposure_start_datetime',
        'drug_exposure_end_date', 'drug_exposure_end_datetime',
        'drug_type_concept_id', 'stop_reason', 'refills', 'route_concept_id', 'lot_number',
        'drug_source_value', 'drug_source_concept_id', 'route_source_value',
        'dose_unit_source_value',
        'filename'
    ])

    df = ctx.spark_session.createDataFrame(df.rdd, ds_schema.domain_dataset_schema['Drug'])
    return(df)

 