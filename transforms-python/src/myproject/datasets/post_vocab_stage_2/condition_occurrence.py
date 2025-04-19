from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output

#https://stackoverflow.com/questions/39235704/split-spark-dataframe-string-column-into-multiple-columns

@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/condition_occurrence"),
    source_df=Input("ri.foundry.main.dataset.e34c8928-d1c1-4b4e-8026-e6024e6afdbb"),
    codemap = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk")
)
def compute(source_df, codemap):
    split_source_value = F.split(source_df.condition_source_value, '\\|') # splits on a regex, escape the 'or'
    df = source_df.withColumn('condition_concept_source_system', split_source_value.getItem(1)) \
                  .withColumn('condition_concept_source_code', split_source_value.getItem(0))

    df = df.join(codemap, (df.condition_concept_source_system == codemap.src_vocab_code_system) & \
                          (df.condition_concept_source_code == codemap.src_code) ) 
    df = df.select('condition_source_value', 'condition_concept_source_system', 'condition_concept_source_code', \
                        'target_concept_id', 'target_domain_id', 'source_concept_id')


    return df

