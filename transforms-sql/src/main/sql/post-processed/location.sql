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
        map.data_partner_id
    FROM `ri.foundry.main.dataset.1ed07a69-4970-4b8d-b67c-e0557606ff33` l
    JOIN   `ri.foundry.main.dataset.672dd7ae-bbd4-43e8-9b8b-b5c7e8711e79` rm
      ON l.filename = rm.response_file_path
    JOIN  `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON rm.healthcare_site = map.healthcare_site