from transforms.api import Input, Output, transform, configure
from pyspark.sql import types as T
import logging 
import pandas as pd
import re
import traceback

from prototype_2 import layer_datasets
from myproject.util.ds_schema import domain_dataset_schema
from myproject.util.omop_eav_dict_common import get_valueset_dict_list
from myproject.util.omop_eav_dict_common import get_visitmap_dict_list
from myproject.util.omop_eav_dict_common import get_codemap_dict_list


logger = logging.getLogger(__name__)


#@configure(profile=['DRIVER_MEMORY_EXTRA_LARGE', 'EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_64'])
@transform(
    dq_person = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/dq_person"),
    dq_visit = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/dq_visit"),
    dq_measurement = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/dq_measurement"),
    dq_observation = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/dq_observation"),
    dq_drug = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/dq_drug"),
    dq_procedure = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/dq_procedure"),
    dq_care_site = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/dq_care_site"),
    dq_location = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/dq_location"),
    dq_provider = Output("/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/dq_provider"),
    input_files=Input("ri.foundry.main.dataset.877bc6a8-2ec1-4b21-9794-4ad02cc27e30"), # the test doc.s
    codemap_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/codemap_xwalk"),
    visit_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/visit_concept_xwalk_mapping_dataset"),
    valueset_xwalk_ds = Input("/All of Us-cdb223/HIN - HIE/CCDA/transform/mapping-reference-files/ccda_value_set_mapping_table_dataset") )
def compute(ctx,
    dq_person, dq_visit, dq_measurement, dq_observation, dq_drug, dq_procedure,
    dq_care_site, dq_location, dq_provider,
    input_files,
    codemap_xwalk_ds,  visit_xwalk_ds, valueset_xwalk_ds
    ):

    codemap_dict = get_codemap_dict_list(codemap_xwalk_ds)
    value_set_map_dict = get_valueset_dict_list(valueset_xwalk_ds)
    visit_map_dict = get_visitmap_dict_list(visit_xwalk_ds)

    # We do not call process_file() from here b/c this is Spark, and that is Jupyter. The file
    # access is different.
    input_fs = input_files.filesystem() # with just iput_files.filesystem I get an error about a function not having an attribute
    documents_list = list(input_fs.ls())
    doc_regex = re.compile(r'(<ClinicalDocument.*?</ClinicalDocument>)', re.DOTALL)

    domain_datasets_dict = {}
    for file_status in documents_list:
        path = file_status.path
        with input_fs.open(path) as file:
            content = file.read()

        # FOR EACH "DOC" in this row (hopefully just 1)
        for match in doc_regex.finditer(content):
            instance_content = match.groups(0)[0]
            omop_data = layer_datasets.process_string_to_dict(instance_content, path, False, 
                       codemap_dict, visit_map_dict, value_set_map_dict)
            logger.info(f"omop_dq.compute() omop_data keys {omop_data.keys()}") 
            for key in omop_data:
                if omop_data[key] is not None:
                    logger.info(f"omop_dq.compute()  omop_data[{key}]  {len(omop_data[key])}") 
                else:
                    logger.error(f"omop_dq.compute()  omop_data[{key}]  NONE") 

            omop_datasets_dict = layer_datasets.create_omop_domain_dataframes(omop_data, path)
            logger.info(f"omop_dq.compute()  omop_datasets keys {omop_datasets_dict.keys()}") 
            for key in omop_datasets_dict:
                if key in omop_datasets_dict and omop_datasets_dict[key] is not None:
                    logger.info(f"omop_dq.compute()  omop_datasets_dict[{key}]  {len(omop_datasets_dict[key])}") 
                else:
                    logger.error(f"omop_dq.compute()  omop_datasets_dict[{key}] is NONE") 
            
            new_domain_datasets_dict = layer_datasets.combine_datasets(omop_datasets_dict)
            logger.info(f"omop_dq.compute()  domain_datasets keys {omop_datasets_dict.keys()}") 
            for key in new_domain_datasets_dict:
                if key in new_domain_datasets_dict and new_domain_datasets_dict[key] is not None:
                    logger.info(f"omop_dq.compute()  new_domain_datasets_dict[{key}]  {len(new_domain_datasets_dict[key])}") 
                else:
                    logger.error(f"omop_dq.compute()  new_domain_datasets_dict[{key}] is NONE") 

        for key in new_domain_datasets_dict:
            if new_domain_datasets_dict[key] is not None:
                if key in domain_datasets_dict:
                    domain_datasets_dict[key] = pd.concat([domain_datasets_dict[key], new_domain_datasets_dict[key]])
                    logger.info(f"omop_dq.compute() domain_datasets_dict[{key}]  {len(domain_datasets_dict[key])}") 
                else:
                    domain_datasets_dict[key] = new_domain_datasets_dict[key]
                    logger.error(f"omop_dq.compute() domain_datasets_dict[{key}] is NONE") 


    logger.error(f" domain_datasets keys {domain_datasets_dict.keys()}") 

    person_fields = ['person_id', 'gender_concept_id', 'year_of_birth', 'month_of_birth', 'day_of_birth', 
        'birth_datetime', 'race_concept_id', 'ethnicity_concept_id', 'location_id', 
        'provider_id',  'care_site_id',  'person_source_value',  'gender_source_value', 
        'gender_source_concept_id',  'race_source_value',  'race_source_concept_id', 
        'ethnicity_source_value',  'ethnicity_source_concept_id',  'filename']
    try:
        if 'Person' in domain_datasets_dict:
            df = ctx.spark_session.createDataFrame(domain_datasets_dict['Person'][person_fields], 
                domain_dataset_schema['Person'])
            logger.info(f"saving person  {len(domain_datasets_dict['Person'])}  {df.count()}")
            dq_person.write_dataframe(df)
        else:
            logger.warning("omop_dq.compute() no Person data")
    except Exception as e:
        logger.error(f" Person threw {e} TB:{traceback.format_exc(e)}")

    visit_fields = [ 'visit_source_value',  'person_id',  'visit_occurrence_id', 'visit_source_concept_id', 
        'preceding_visit_occurrence_id', 'discharge_to_concept_id',  'admitting_source_concept_id', 
        'care_site_id',  'provider_id',  'visit_concept_id',  'visit_start_date',  'visit_start_datetime', 
        'visit_end_date',  'visit_end_datetime',  'visit_type_concept_id', 'admitting_source_value',
        'discharge_to_source_value', 'filename' ]
    try:
        if 'Visit' in domain_datasets_dict:
            df = ctx.spark_session.createDataFrame(domain_datasets_dict['Visit'][visit_fields], 
                domain_dataset_schema['Visit'])
            logger.info(f"saving visit {len(domain_datasets_dict['Visit'])} {df.count()}")
            dq_visit.write_dataframe(df)
        else:
            logger.warning("omop_dq.compute() no Visit data")
    except Exception as e:
        logger.error(f" Visit threw {e} TB:{traceback.format_exc(e)}")

    measurement_fields = ['measurement_time', 'visit_detail_id', 'visit_occurrence_id', 'provider_id',
        'range_high', 'range_low', 'unit_concept_id', 'person_id', 'measurement_id',
        'measurement_concept_id', 'measurement_date', 'measurement_datetime',
        'measurement_type_concept_id', 'operator_concept_id', 'value_as_number',
        'value_as_concept_id', 'measurement_source_value', 'measurement_source_concept_id',
        'unit_source_value', 'value_source_value', 'filename' ]
    try:
        if 'Measurement' in domain_datasets_dict:
            df = ctx.spark_session.createDataFrame(domain_datasets_dict['Measurement'][measurement_fields], 
                domain_dataset_schema['Measurement'])
            logger.info(f"saving measurement {len(domain_datasets_dict['Measurement'])} {df.count()}")
            dq_measurement.write_dataframe(df)
        else:
            logger.warning("omop_dq.compute() no Measurement data")
    except Exception as e:
        logger.error(f" Measurement threw {e} TB:{traceback.format_exc(e)}")

    observation_fields = [ 'visit_detail_id', 'visit_occurrence_id', 'provider_id', 'value_as_number',
        'person_id', 'observation_id', 'observation_concept_id', 'observation_source_value',
        'observation_source_concept_id', 'observation_date', 'observation_datetime',
        'observation_type_concept_id', 'value_as_string', 'value_as_concept_id', 'qualifier_concept_id',
        'qualifier_source_value', 'unit_concept_id', 'unit_source_value', 'filename' ]
    try:
        if 'Observation' in domain_datasets_dict:
            df = ctx.spark_session.createDataFrame(domain_datasets_dict['Observation'][observation_fields], 
                 domain_dataset_schema['Observation'])
            logger.info(f"saving observation {len(domain_datasets_dict['Observation'])}  {df.count()}")
            dq_observation.write_dataframe(df)
        else:
            logger.warning("omop_dq.compute() no Observation data")
    except Exception as e:
        logger.error(f" Observation threw {e} TB:{traceback.format_exc(e)}")

    drug_fields = [ 'visit_detail_id', 'sig', 'verbatim_end_date', 'visit_occurrence_id', 'provider_id', # 5
        'days_supply', 'quantity', 'person_id', 'drug_exposure_id', 'drug_concept_id',                   # 5
        'drug_exposure_start_date', 'drug_exposure_start_datetime', 'drug_exposure_end_date',            # 3
        'drug_exposure_end_datetime', 'drug_type_concept_id', 'stop_reason', 'refills',                  # 4
        'route_concept_id',
        'lot_number', 'drug_source_value', 'drug_source_concept_id', 'route_source_value',               # 4
        'dose_unit_source_value', 'filename' ]                                                           # 2
                                                                                                          # 23 
    try:
        if 'Drug' in domain_datasets_dict:
            df = ctx.spark_session.createDataFrame(domain_datasets_dict['Drug'][drug_fields], 
                domain_dataset_schema['Drug'])
            logger.info(f"saving drug {len(domain_datasets_dict['Drug'])}  {df.count()}")
            dq_drug.write_dataframe(df)
        else:
            logger.warning("omop_dq.compute() no Drug data")
    except Exception as e:
        logger.error(f" Drug threw {e} TB:{traceback.format_exc(e)}")

    procedure_fields = [ 'visit_detail_id', 'procedure_source_concept_id', 'procedure_concept_id',
        'visit_occurrence_id', 'person_id', 'procedure_occurrence_id', 'provider_id',
        'modifier_concept_id', 'procedure_date', 'procedure_datetime', 'procedure_type_concept_id',
        'quantity', 'procedure_source_value', 'modifier_source_value', 'filename' ]
    try:
        if 'Procedure' in domain_datasets_dict:
            df = ctx.spark_session.createDataFrame(domain_datasets_dict['Procedure'][procedure_fields], 
                domain_dataset_schema['Procedure'])
            logger.info(f"saving procedure {len(domain_datasets_dict['Procedure'])}  {df.count()}")
            dq_procedure.write_dataframe(df)
        else:
            logger.warning("omop_dq.compute() no Procedure data")
    except Exception as e:
         logger.error(f" Procedure threw {e} TB:{traceback.format_exc(e)} ")


    care_site_fields = [ 'location_id', 'care_site_id', 'care_site_name',
        'place_of_service_concept_id', 'care_site_source_value',
        'place_of_service_source_value', 'filename',
    ]
    try:
        if 'Care_Site' in domain_datasets_dict:
            df = ctx.spark_session.createDataFrame(domain_datasets_dict['Care_Site'][care_site_fields], 
                domain_dataset_schema['Care_Site'])
            logger.info(f"saving Care_Site {len(domain_datasets_dict['Care_Site'])}  {df.count()}")
            dq_care_site.write_dataframe(df)
        else:
            logger.warning("omop_dq.compute() no Care_Site data")
    except Exception as e:
         logger.error(f" Care_Site threw {e} TB:{traceback.format_exc(e)}")

    location_fields = [ 'location_id', 'address_1', 'address_2',
        'city', 'state', 'zip', 'county',
        'location_source_value', 'filename',
    ]
    try:
        if 'Location' in domain_datasets_dict:
            df = ctx.spark_session.createDataFrame(domain_datasets_dict['Location'][location_fields], 
                domain_dataset_schema['Location'])
            logger.info(f"saving Location {len(domain_datasets_dict['Location'])}  {df.count()}")
            dq_location.write_dataframe(df)
        else:
            logger.warning("omop_dq.compute() no Procedure data")
    except Exception as e:
         logger.error(f" Procedure threw {e} TB:{traceback.format_exc(e)} ")

    provider_fields = [ 'year_of_birth', 'care_site_id', 'provider_id', 'provider_name',
        'npi', 'dea', 'specialty_concept_id', 'gender_concept_id', 'provider_source_value',
        'specialty_source_value', 'specialty_source_concept_id', 'gender_source_value',
        'gender_source_concept_id', 'filename',
    ]
    try:
        if 'Provider' in domain_datasets_dict:
            df = ctx.spark_session.createDataFrame(domain_datasets_dict['Provider'][provider_fields], 
                domain_dataset_schema['Provider'])
            logger.info(f"saving procedure {len(domain_datasets_dict['Provider'])}  {df.count()}")
            dq_provider.write_dataframe(df)
        else:
            logger.warning("omop_dq.compute() no Provider data")
    except Exception as e:
         logger.error(f" Provider threw {e} TB:{traceback.format_exc(e)} ")
