from pyspark.sql import functions as F
from pyspark.sql import types as T
from transforms.api import transform_df, Input, Output
from ..util import ds_schema


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/device_exposure"),
    omop_eav_dict=Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/omop_eav_dict_May10"),
)
def compute(ctx, omop_eav_dict):
    df = omop_eav_dict.select('key_value', 'field_name', 'field_value') \
                .where(F.column('domain_name') == 'Device') \
                .distinct() \
                .groupBy('key_value') \
                .pivot("field_name") \
                .agg(F.first('field_value')) \
                .drop('key_value')

    df = df \
        .withColumn('visit_detail_id', df['visit_detail_id'].cast(T.LongType())) \
        .withColumn('visit_occurrence_id', df['visit_occurrence_id'].cast(T.LongType())) \
        .withColumn('provider_id', df['provider_id'].cast(T.LongType())) \
        .withColumn('person_id', df['person_id'].cast(T.LongType())) \
        .withColumn('device_exposure_id', df['device_exposure_id'].cast(T.LongType())) \
        .withColumn('device_concept_id', df['device_concept_id'].cast(T.IntegerType())) \
        .withColumn('device_exposure_start_date', F.to_date(F.col('device_exposure_start_date'))) \
        .withColumn('device_exposure_start_datetime', F.to_timestamp(F.col('device_exposure_start_datetime'))) \
        .withColumn('device_exposure_end_date', F.to_date(F.col('device_exposure_end_date'))) \
        .withColumn('device_exposure_end_datetime', F.to_timestamp(F.col('device_exposure_end_datetime'))) \
        .withColumn('device_type_concept_id', df['device_type_concept_id'].cast(T.IntegerType())) \
        .withColumn('unique_device_id', df['unique_device_id'].cast(T.IntegerType())) \
        .withColumn('quantity', df['quantity'].cast(T.IntegerType())) \
        .withColumn('device_source_value', df['device_source_value'].cast(T.StringType())) \
        .withColumn('device_source_concept_id', df['device_source_concept_id'].cast(T.IntegerType())) \

    df = df.select([ 
        'visit_detail_id',
        'visit_occurrence_id', 'provider_id',
        'person_id', 'device_exposure_id', 'device_concept_id',
        'device_exposure_start_date', 'device_exposure_start_datetime',
        'device_exposure_end_date', 'device_exposure_end_datetime',
        'device_type_concept_id', 'unique_device_id',
        'quantity', 'device_source_value', 'device_source_concept_id',
        'filename'
    ])

    df = ctx.spark_session.createDataFrame(df.rdd, ds_schema.domain_dataset_schema['Device'])
    return (df)
