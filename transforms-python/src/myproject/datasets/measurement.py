from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from ..util import ds_schema
from pyspark.sql import types as T

@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/measurement"),
    omop_eav_dict = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/omop_eav_dict"),
)
def compute(ctx, omop_eav_dict):
    df = omop_eav_dict.select('key_value', 'field_name', 'field_value') \
                .where(F.column('domain_name') == 'Measurement') \
                .distinct() \
                .groupBy('key_value') \
                .pivot("field_name") \
                .agg(F.first('field_value')) \
                .drop('key_value')

    df = df \
        .withColumn('visit_detail_id', df['visit_detail_id'].cast(T.LongType())) \
        .withColumn('visit_occurrence_id', df['visit_occurrence_id'].cast(T.LongType())) \
        .withColumn('provider_id', df['provider_id'].cast(T.LongType())) \
        .withColumn('range_high', df['range_high'].cast(T.FloatType())) \
        .withColumn('range_low', df['range_low'].cast(T.FloatType())) \
        .withColumn('unit_concept_id', df['unit_concept_id'].cast(T.IntegerType()))  \
        .withColumn('person_id', df['person_id'].cast(T.LongType())) \
        .withColumn('measurement_id', df['measurement_id'].cast(T.LongType())) \
        .withColumn('measurement_concept_id', df['measurement_concept_id'].cast(T.IntegerType())) \
        .withColumn('measurement_date',  F.to_date(F.col('measurement_date'))) \
        .withColumn('measurement_datetime',  F.to_timestamp(F.col('measurement_datetime'))) \
        .withColumn('measurement_type_concept_id', df['measurement_type_concept_id'].cast(T.IntegerType())) \
        .withColumn('operator_concept_id', df['operator_concept_id'].cast(T.IntegerType())) \
        .withColumn('value_as_number', df['value_as_number'].cast(T.DoubleType())) \
        .withColumn('value_as_concept_id', df['value_as_concept_id'].cast(T.IntegerType())) \
        .withColumn('measurement_source_concept_id', df['measurement_source_concept_id'].cast(T.IntegerType())) 
        
        #.withColumn('measurement_time',  F.to_timestamp(F.col('measurement_time'))) \    # STRING?

    new_df = ctx.spark_session.createDataFrame(df.rdd, ds_schema.domain_dataset_schema['Measurement'])
    return(df)

