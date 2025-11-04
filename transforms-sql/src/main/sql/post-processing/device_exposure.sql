CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/device_exposure` AS
    SELECT  distinct
      de.device_exposure_id,
      cast(rm.mspi as LONG) as person_id,
      de.device_concept_id,
      to_date(de.device_exposure_start_date) as device_exposure_start_date,
      de.device_exposure_start_datetime,
      to_date(de.device_exposure_end_date) as device_exposure_end_date,
      de.device_exposure_end_datetime,

      de.unique_device_id,
      de.device_type_concept_id,
      de.quantity,
      de.provider_id,

      de.visit_occurrence_id,
      de.visit_detail_id,  
      de.device_source_value, 
      de.device_source_concept_id, 
      map.data_partner_id
--       /All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/device_exposure

    -- FROM  `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post_vocab_stage_2/device_exposure` de
    FROM  `/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/device_exposure` de
    JOIN   `/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata` rm
      ON de.filename = rm.response_file_path
    JOIN  `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON rm.healthcare_site = map.healthcare_site

