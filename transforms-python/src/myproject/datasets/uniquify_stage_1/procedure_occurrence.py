from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType
from transforms.api import transform_df, Input, Output

from .stage_functions import choose_most_data


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/procedure_occurrence"),
    source_df = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/procedure_occurrence"),
)
def compute(source_df):
    PK=["procedure_occurrence_id", "person_id", "provider_id", "procedure_date", "procedure_datetime",
             "procedure_source_value", "filename"]
             ###"filename", "procedure_source_value"]


    df_deduplicated = choose_most_data(
        source_df,
        primary_key_fields=PK,
        preference_fields=["visit_occurrence_id"]
    )

    return df_deduplicated
