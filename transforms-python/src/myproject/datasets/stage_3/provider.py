# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/stage_3/provider"),
    source_df=Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/stage_2/provider")
)
def compute(source_df):
    return source_df
