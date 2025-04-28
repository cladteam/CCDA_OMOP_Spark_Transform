from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/care_site"),
    source_df = Input("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/care_site"),
    codemap=Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk")
)
def compute(source_df, codemap):
    split_source_value = F.split(source_df.place_of_service_source_value, '\\|') # splits on a regex, escape the 'or'
    df = source_df.withColumn('place_of_service_source_system', split_source_value.getItem(1)) \
                  .withColumn('place_of_service_source_code', split_source_value.getItem(0))

    df = df.join(codemap, (df.place_of_service_source_system == codemap.src_vocab_code_system) & \
                          (df.place_of_service_source_code == codemap.src_code),
                          "leftouter")

    df = df.withColumn('place_of_service_concept_id', df.target_concept_id)

    df = df.drop('place_of_service_source_system')
    df = df.drop('place_of_serivce_source_code')


    df = df.select(['location_id', 'care_site_id', 'care_site_name', 'place_of_service_concept_id',
                    'care_site_source_value', 'place_of_service_source_value',
                    'filename'])

    return df
