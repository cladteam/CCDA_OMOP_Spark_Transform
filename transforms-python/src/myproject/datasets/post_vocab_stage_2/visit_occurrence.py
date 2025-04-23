from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/visit_occurrence"),
    visits = Input("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/visit_occurrence")
)
def compute(visits):
    
    df = visits.select([
        'visit_source_value', 'person_id', 'visit_occurrence_id', 'visit_source_concept_id',
        'preceding_visit_occurrence_id', 'discharge_to_concept_id', 'admitting_source_concept_id',
        'care_site_id', 'provider_id', 'visit_concept_id', 'visit_start_date', 'visit_start_datetime',
        'visit_end_date', 'visit_end_datetime', 'visit_type_concept_id', 'admitting_source_value',
        'discharge_to_source_value', 
        'filename'
    ]) 

    return df
