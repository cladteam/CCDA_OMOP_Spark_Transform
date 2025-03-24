# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


dataset_name  = 'ccda_response_files'

@transform_df(
    condition_occurrence = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/condition_occurrence"),
    drug_exposure = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/drug_exposure"),
    location = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/location"),
    measurement = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/measurement"),
    observation = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/observation"),
    person = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/person"),
    procedure_occurrence = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/procedure_occurrence"),
    provider = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/provider"),
    visit_occurrence = Output("ri.foundry.main.dataset.1fc47371-d39c-4985-aacd-c0aacf5484b3"),

    xml_files = Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentified/ccda_response_files"),
    metadata = Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentified/ccda_response_metadata"),
    visit_xwalk = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
    codemap_xwalk = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk"),
    valueset_xwalk = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset"),
)

def compute(
    condition_occurrence,  drug_exposure, location, 
    measurement, observation, person,
    procedure_occurrence, provider, visit_occurrence,
    xml_files, metadata, visit_xwalk,
    codemap_xwalk, valueset_xwalk ):

    
