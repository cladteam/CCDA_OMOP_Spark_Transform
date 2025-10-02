from pyspark.sql import functions as F
from pyspark.sql import types as T
from transforms.api import transform_df, Input, Output
from ..util import ds_schema

@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/person"),
    omop_eav_dict = Input("ri.foundry.main.dataset.380d0b16-2268-4afc-85b4-12193705af00"), 
)
def compute(ctx, omop_eav_dict):
                
    
    df = omop_eav_dict.select('key_value', 'field_name', 'field_value') \
                .where(F.column('domain_name') == 'Person') \
                .distinct() \
                .groupBy('key_value') \
                .pivot("field_name") \
                .agg(F.first('field_value')) \
                .drop('key_value')

    df = df \
        .withColumn('person_id', df['person_id'].cast(T.LongType())) \
        .withColumn('gender_concept_id', df['gender_concept_id'].cast(T.IntegerType())) \
        .withColumn('year_of_birth', df['year_of_birth'].cast(T.IntegerType())) \
        .withColumn('month_of_birth', df['month_of_birth'].cast(T.IntegerType())) \
        .withColumn('day_of_birth', df['day_of_birth'].cast(T.IntegerType())) \
        .withColumn('birth_datetime', F.to_timestamp(F.col('birth_datetime'), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn('race_concept_id', df['race_concept_id'].cast(T.IntegerType())) \
        .withColumn('ethnicity_concept_id', df['ethnicity_concept_id'].cast(T.IntegerType())) \
        .withColumn('location_id', df['location_id'].cast(T.LongType())) \
        .withColumn('provider_id', df['provider_id'].cast(T.LongType())) \
        .withColumn('care_site_id', df['care_site_id'].cast(T.LongType())) \
        .withColumn('gender_source_concept_id', df['gender_source_concept_id'].cast(T.IntegerType())) \
        .withColumn('race_source_concept_id', df['race_source_concept_id'].cast(T.IntegerType())) \
        .withColumn('ethnicity_source_concept_id', df['ethnicity_source_concept_id'].cast(T.IntegerType())) \
        .replace("None", None, subset=['ethnicity_source_value', 'gender_source_value', 'race_source_value']) # schema

    df = df.select([
        'person_id', 'gender_concept_id',  'year_of_birth',  'month_of_birth',  'day_of_birth',
        'birth_datetime',  'race_concept_id',  'ethnicity_concept_id',  'location_id',  'provider_id',
        'care_site_id',  'person_source_value',  'gender_source_value',  'gender_source_concept_id',
        'race_source_value',  'race_source_concept_id',  'ethnicity_source_value',
        'ethnicity_source_concept_id',
        'filename'
    ])

    df = ctx.spark_session.createDataFrame(df.rdd, ds_schema.domain_dataset_schema['Person'])

    return(df)
 