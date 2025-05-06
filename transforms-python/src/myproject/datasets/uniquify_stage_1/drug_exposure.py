from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType
from transforms.api import transform_df, Input, Output

from .stage_functions import choose_most_data


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/drug_exposure"),
    source_df = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/drug_exposure")
)
def compute(source_df):

    # Step 2: Deduplicate based on primary key and preference fields
    df_deduplicated = choose_most_data(
        source_df,
        primary_key_fields=["drug_exposure_id", "person_id", "drug_exposure_start_date", "filename"],
        preference_fields=["visit_occurrence_id", "drug_source_value"]
    )

    return df_deduplicated

