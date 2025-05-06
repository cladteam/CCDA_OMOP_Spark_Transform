from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType
from transforms.api import transform_df, Input, Output

from .stage_functions import choose_most_data


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/observation"),
    source_df = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/observation"),
)
def compute(source_df):
    PK=["observation_id", "person_id", "provider_id", "observation_date", "filename"]
             ###"filename", "observation_source_value"]


    df_deduplicated = choose_most_data(
        source_df,
        primary_key_fields=PK,
        preference_fields=["visit_occurrence_id", "observation_datetime", "observation_source_value"]
        # this only helped the observation_source_value.
    )

    return df_deduplicated
