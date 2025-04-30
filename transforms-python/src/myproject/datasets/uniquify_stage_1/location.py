from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType
from transforms.api import transform_df, Input, Output

@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/location"),
    source_df = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/location")
)
def compute(source_df):

    # Step 1: Remove exact duplicates across all columns
    df_deduplicate = source_df.dropDuplicates()

    return df_deduplicate
