from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType
from transforms.api import transform_df, Input, Output
from .stage_functions import choose_most_data




@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/device_exposure"),
    source_df = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/device_exposure")
)
def compute(source_df):

    # Step 1: Remove exact duplicates across all columns
    df_deduplicate = source_df.dropDuplicates()

    # Step 2: Deduplicate based on primary key and preference fields
    df_deduplicate_by_primary_key = choose_most_data(
        df_deduplicate,
        primary_key_fields=["device_exposure_id", "person_id", "device_concept_id", "device_exposure_start_date",
                     "filename"],
        preference_fields=["visit_occurrence_id", "device_source_value"]
    )

    return df_deduplicate_by_primary_key
