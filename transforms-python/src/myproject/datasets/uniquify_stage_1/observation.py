# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/observation"),
    source_df = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/observation"),
)
def compute(source_df):
    return source_df.dropDuplicates()
