CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/care_site` AS
    SELECT distinct
        cs.care_site_id,
        cs.care_site_name,
        cs.place_of_service_concept_id,
        cs.location_id,
        cs.care_site_source_value,
        cs.place_of_service_source_value,
        map.data_partner_id
    FROM `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/care_site` cs
    JOIN   `ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79` rm
      ON cs.filename = rm.response_file_path
    JOIN  `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON rm.healthcare_site = map.healthcare_site
      -- /All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata
      -- ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79
