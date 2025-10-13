from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from ..util import ds_schema
from pyspark.sql import types as T
from . import OMOP_EAV_DICT_FULL_PATH, OUTPUT_FULL_BASE_PATH

@transform_df(
    Output(f"{OUTPUT_FULL_BASE_PATH}/procedure_occurrence"),
    omop_eav_dict=Input(OMOP_EAV_DICT_FULL_PATH),
)
def compute(ctx, omop_eav_dict):
    df = omop_eav_dict.select('key_value', 'field_name', 'field_value') \
                .where(F.column('domain_name') == 'Procedure') \
                .distinct() \
                .groupBy('key_value') \
                .pivot("field_name") \
                .agg(F.first('field_value')) \
                .drop('key_value')

    df = df \
        .withColumn('visit_detail_id', df['visit_detail_id'].cast(T.LongType())) \
        .withColumn('procedure_source_concept_id', df['procedure_source_concept_id'].cast(T.IntegerType())) \
        .withColumn('procedure_concept_id', df['procedure_concept_id'].cast(T.IntegerType())) \
        .withColumn('visit_occurrence_id', df['visit_occurrence_id'].cast(T.LongType())) \
        .withColumn('person_id', df['person_id'].cast(T.LongType())) \
        .withColumn('procedure_occurrence_id', df['procedure_occurrence_id'].cast(T.LongType())) \
        .withColumn('provider_id', df['provider_id'].cast(T.LongType())) \
        .withColumn('modifier_concept_id', df['modifier_concept_id'].cast(T.IntegerType())) \
        .withColumn('procedure_date',  F.to_date(F.col('procedure_date'))) \
        .withColumn('procedure_datetime',  F.to_timestamp(F.col('procedure_datetime'))) \
        .withColumn('procedure_type_concept_id', df['procedure_type_concept_id'].cast(T.IntegerType())) \
        .withColumn('quantity', df['quantity'].cast(T.IntegerType())) 

    df = df.select([
        'visit_detail_id', 'procedure_source_concept_id', 'procedure_concept_id', 'visit_occurrence_id',
        'person_id', 'procedure_occurrence_id', 'provider_id', 'modifier_concept_id',
        'procedure_date', 'procedure_datetime', 'procedure_type_concept_id',
        'quantity', 'procedure_source_value', 'modifier_source_value',
        'filename', 'cfg_name'
    ])
        

    df = ctx.spark_session.createDataFrame(df.rdd, ds_schema.domain_dataset_schema['Procedure'])
    return(df)

