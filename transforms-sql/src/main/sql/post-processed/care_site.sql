CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/care_site` AS
    SELECT distinct
        cs.care_site_id,
        cs.care_site_name,
        cs.place_of_service_concept_id,
        cs.location_id,
        cs.care_site_source_value,
        cs.place_of_service_source_value,
        cs.data_partner_id
    FROM `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/care_site` cs
    WHERE cs.care_site_id IS NOT NULL;

