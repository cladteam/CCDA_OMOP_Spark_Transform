# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/location"),
    source_df = Input("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/location"),
)
def compute(source_df):
    return source_df
