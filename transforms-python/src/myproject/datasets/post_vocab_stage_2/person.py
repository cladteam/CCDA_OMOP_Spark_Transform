from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from pyspark.sql.types import IntegerType


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/person"),
    df = Input("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/person"),
    valueset_map = Input("ri.foundry.main.dataset.5ba1594c-ef36-4eeb-b6c4-d3d005dbd127")
)
def compute(df, valueset_map):

    # GENDER 2.16.840.1.113883.5.1
    df = df.alias('m') \
           .join(valueset_map.alias('cm'), \
                 (df.gender_source_value == valueset_map.src_cd) & \
                 (F.col('cm.codeSystem') == "2.16.840.1.113883.5.1"), \
                 "leftouter") \
           .select('m.*', 'cm.target_concept_id')
    df = df.withColumn('gender_concept_id', df.target_concept_id.cast(IntegerType()))

    df = df. drop("target_concept_id")


    # RACE 2.16.840.1.113883.5.104 
    df = df.alias('m') \
          .join(valueset_map.alias('cm'), \
                 (df.race_source_value == valueset_map.src_cd) & \
                 (F.col('cm.codeSystem') == '2.16.840.1.113883.5.104'), \
                  "leftouter") \
          .select('m.*', 'cm.target_concept_id')
    df = df.withColumn('race_concept_id', df.target_concept_id.cast(IntegerType()))
    df = df. drop("target_concept_id")


    # ETHNICITY 2.16.840.1.113883.6.238
    df = df.alias('m') \
           .join(valueset_map.alias('cm'), \
                 (df.ethnicity_source_value == valueset_map.src_cd) & \
                 (F.col('cm.codeSystem') ==  '2.16.840.1.113883.6.238'), \
                 "leftouter") \
           .select('m.*', 'cm.target_concept_id')
    df = df.withColumn('ethnicity_concept_id', df.target_concept_id.cast(IntegerType()))
    df = df. drop("target_concept_id")

    return(df)
    
