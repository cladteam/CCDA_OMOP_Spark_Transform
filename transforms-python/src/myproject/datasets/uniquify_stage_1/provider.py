from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType
from transforms.api import transform_df, Input, Output
from .stage_functions import choose_most_data


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/provider"),
    source_df = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/provider")
)
def compute(source_df):

    return source_df
