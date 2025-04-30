from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType
from transforms.api import transform_df, Input, Output

from .stage_functions import choose_most_data

@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/visit_occurrence"),
    source_df = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/visit_occurrence")
)
def compute(source_df):

    df_deduplicated = choose_most_data(
        source_df,
        primary_key_fields = "visit_occurrence_id",
        preference_fields = ["visit_concept_id"]
    )

    return df_deduplicated
