
from transforms.api import transform, Input, Output
from pyspark.sql import types as T
from pyspark.sql import functions as F


import os

from prototype_2 import layer_datasets
from prototype_2 import codemap_xwalk
from prototype_2 import ccda_value_set_mapping_table_dataset
from prototype_2 import visit_concept_xwalk_mapping_dataset

@transform(
    care_site = Output("ri.foundry.main.dataset.4c563173-2281-4e0c-99e1-f11ea21f8eb6"),
    condition_occurrence = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/condition_occurrence"),
    drug_exposure = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/drug_exposure"),
    location = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/location"),
    measurement = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/measurement"),
    observation = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/observation"),
    person = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/person"),
    procedure_occurrence = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/procedure_occurrence"),
    provider = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP/provider"),
    visit_occurrence = Output("ri.foundry.main.dataset.1fc47371-d39c-4985-aacd-c0aacf5484b3"),

    xml_files=Input("ri.foundry.main.dataset.8c8ff8f9-d429-4396-baed-a3de9c945f49"),
    # xml_files = Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentified/ccda/ccda_response_files"),
    metadata = Input("ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79"),
    # metadata = Input("/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentified/ccda/ccda_response_metadata"),
    visit_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
    codemap_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk"),
    valueset_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset")
)

def compute(
    ctx,
    # outputs
        care_site,
        condition_occurrence,  drug_exposure, location, 
        measurement, observation, person,
        procedure_occurrence, provider, visit_occurrence,
    # inputs
        xml_files, 
        metadata, visit_xwalk,
        codemap_xwalk, valueset_xwalk ):

    FILE_LIMIT=60 
    EXPORT_DATASETS=False

    # Link concept maps
    # set package variables to these datasets in an obvious way here
    codemap_xwalk = codemap_xwalk_ds
    ccda_value_set_mapping_table_dataset = valueset_xwalk_ds
    visit_concept_xwalk_mapping_dataset = visit_xwalk_ds


    # Process Files 
    filestatus_list = list(xml_files.filesystem().ls())
    file_count=0
    tuple_list = [] # for a status df
    omop_dataset_dict = {}
    fs = xml_files.filesystem()
    doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)
    for status in filestatus_list:
        start_time = time.time()
        with fs.open(status.path, 'rb') as f:
            br = io.BufferedReader(f)
            tw = io.TextIOWrapper(br) 
            contents = tw.readline()
            for line in tw:
                contents += line
            for match in doc_regex.finditer(contents):            
                match_tuple = match.groups(0)
                new_data_dict = layer_datasets.process_string(match_tuple[0], status.path, False )

                # build up the omop_dataset from new_data_dict
                for key in new_data_dict:
                    if key in omop_dataset_dict and omop_dataset_dict[key] is not None:
                        if new_data_dict[key] is  not None:
                            omop_dataset_dict[key] = pd.concat([ omop_dataset_dict[key], new_data_dict[key] ])
                    else:
                        omop_dataset_dict[key]= new_data_dict[key]
                    if new_data_dict[key] is not None:
                        logger.info(f"{file} {key} {len(omop_dataset_dict)} {omop_dataset_dict[key].shape} {new_data_dict[key].shape}")
                    else:
                        logger.info(f"{file} {key} {len(omop_dataset_dict)} None / no data")

            end_time = time.time()
            time_int  = end_time - start_time
            string_length  =  len(contents)
            tuple_list.append([status.path, status.size, time_int, string_length, contents])
        file_count += 1
        if file_count > FILE_LIMIT:
            break

    domain_dataset_dict = combine_datasets(omop_dataset_dict)

    #care_site.write_dataframe(omop_dataset_dict['Care_Site'])
    if False:
        condition_occurrence.write_dataframe(omop_dataset_dict['Condition'])
        drug_exposure.write_dataframe(omop_dataset_dict['Drug'])
        location.write_dataframe(omop_dataset_dict['Location'])
        measurement.write_dataframe(omop_dataset_dict['Measurement'])
        observation.write_dataframe(omop_dataset_dict['Observation'])
        person.write_dataframe(omop_dataset_dict['Person'])
        procedure_occurrence.write_dataframe(omop_dataset_dict['Procedure'])
        provider.write_dataframe(omop_dataset_dict['Provider'])
        visit_occurrence.write_dataframe(omop_dataset_dict['Visit'])



    
