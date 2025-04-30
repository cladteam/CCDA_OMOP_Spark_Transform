from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, StringType
from transforms.api import transform_df, Input, Output

from .stage_functions import choose_most_data

# In the Person table we see various combinations of non-null and null values for 
# each of race, ethnicity and gender concept_ids.
#
# Sometimes this is complicated by the rows coming from different filenames.
# adding filename to the key would mean we have a duplicate person_id,
# even if we deduplicate after filename has been converted to data_partner_id.
@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/person"),
    source_df = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/person")
)
def compute(source_df):

    df_deduplicated = choose_most_data(
        source_df,
        #primary_key_fields=["person_id", "filename"],
        primary_key_fields=["person_id"],
        preference_fields=["gender_concept_id", "ethnicity_concept_id", "race_concept_id"]
    )

    return df_deduplicated
