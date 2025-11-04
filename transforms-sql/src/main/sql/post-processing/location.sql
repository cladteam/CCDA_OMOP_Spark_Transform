CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/location` AS
    SELECT distinct
        l.location_id,
        upper(l.address_1) as address_1,
        upper(l.address_2) as address_2,
        upper(l.city) as city,
        upper(l.state) as state,
        l.zip,
        upper(l.county) as county,
        l.location_source_value,
--         rm.healthcare_site,
        map.data_partner_id
    -- FROM `ri.foundry.main.dataset.5074cf0d-1c05-4cea-be66-38f91a62fd7a` l
    FROM `/All of Us-cdb223/HIN - HIE/CCDA/datasets/uniquify_stage_1/location` l
    JOIN   `/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata` rm
      ON l.filename = rm.response_file_path
    JOIN  `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON rm.healthcare_site = map.healthcare_site