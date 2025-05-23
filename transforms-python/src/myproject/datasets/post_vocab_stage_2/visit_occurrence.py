from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/visit_occurrence"),
    visits =  Input("/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/visit_occurrence"),
    visit_map = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset")

)
def compute(visits, visit_map):
    #split_source_value = F.split(visits.visit_source_value, '\\|') # splits on a regex, escape the 'or'
    split_source_value = F.split(visits.visit_source_value, '|') # splits on a regex, escape the 'or'
    df = visits.withColumn('visit_concept_source_system', split_source_value.getItem(0)) \
               .withColumn('visit_concept_source_code', split_source_value.getItem(1))

    df = df.alias('v') \
           .join(visit_map.alias('vm'), \
                 (df.visit_concept_source_system == visit_map.codeSystem) & \
                 (df.visit_concept_source_code == visit_map.src_cd), \
                 "leftouter") 
#           .select('v.*', 'vm.target_concept_id') 

    df = df.withColumn('visit_concept_id', df.target_concept_id)
#    df = df.drop('visit_concept_source_system')
#    df = df.drop('visit_concept_source_code')

    
#    df = visits.select([
#        'visit_source_value', 'person_id', 'visit_occurrence_id', 'visit_source_concept_id',
#        'preceding_visit_occurrence_id', 'discharge_to_concept_id', 'admitting_source_concept_id',
#        'care_site_id', 'provider_id', 'visit_concept_id', 'visit_start_date', 'visit_start_datetime',
#        'visit_end_date', 'visit_end_datetime', 'visit_type_concept_id', 'admitting_source_value',
#        'discharge_to_source_value', 
#        'filename'
#    ]) 

    return df
