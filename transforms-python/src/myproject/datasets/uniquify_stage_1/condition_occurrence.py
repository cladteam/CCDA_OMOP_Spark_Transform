from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType
from transforms.api import transform_df, Input, Output
from .stage_functions import choose_most_data


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/condition_occurrence"),
    source_df = Input("ri.foundry.main.dataset.e34c8928-d1c1-4b4e-8026-e6024e6afdbb")
)
def compute(source_df):

    df_deduplicated = choose_most_data(
        source_df,
        primary_key_fields=["condition_occurrence_id" , "person_id", "condition_concept_id",  
                   "condition_start_date", "visit_occurrence_id", "condition_source_value",  
                   "filename"],
        preference_fields=["visit_occurrence_id" ]
    )

    return df_deduplicated
