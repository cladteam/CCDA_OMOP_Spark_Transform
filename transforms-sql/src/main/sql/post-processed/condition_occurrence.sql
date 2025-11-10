CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/condition_occurrence` AS
    SELECT distinct
        co.condition_occurrence_id, --
        cast(rm.mspi as LONG) as person_id, --
        condition_concept_id,
        to_date(co.condition_start_date) as condition_start_date, --
        co.condition_start_datetime, --
        to_date(co.condition_end_date) as condition_end_date, --
        co.condition_end_datetime, --
        co.condition_type_concept_id, --
        co.condition_status_concept_id, --
        co.stop_reason, --
        co.provider_id,
        co.visit_occurrence_id, --
        co.visit_detail_id,--
        co.condition_source_value, --
        co.condition_source_concept_id, --
        co.condition_status_source_value, --
        map.data_partner_id,
        ccn.concept_name as condition_concept_name
--        ctcn.concept_name as condition_type_concept_name,
--        cstcn.concept_name as condition_status_concept_name,
--       csocn.concept_name as condition_source_concept_name
    FROM `ri.foundry.main.dataset.e34c8928-d1c1-4b4e-8026-e6024e6afdbb` co
    JOIN   `/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata` rm
      ON co.filename = rm.response_file_path
    JOIN  `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON rm.healthcare_site = map.healthcare_site
LEFT    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` ccn
      ON co.condition_concept_id = ccn.concept_id
--    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` ctcn
--      ON co.condition_type_concept_id = ctcn.concept_id
--    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` cstcn
--      ON co.condition_status_concept_id = cstcn.concept_id
--    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` csocn
--      ON co.condition_source_concept_id = csocn.concept_id
      