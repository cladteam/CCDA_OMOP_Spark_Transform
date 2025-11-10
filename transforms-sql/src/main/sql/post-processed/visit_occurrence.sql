CREATE TABLE `/All of Us-cdb223/HIN - HIE/CCDA/IdentifiedData/OMOP_spark/post-processed/visit_occurrence` AS
    SELECT distinct
        vo.visit_occurrence_id,
        cast(rm.mspi as LONG) as person_id,
        vo.visit_concept_id,
        to_date(vo.visit_start_date) as visit_start_date,
        vo.visit_start_datetime,
        to_date(vo.visit_end_date) as visit_end_date,
        vo.visit_end_datetime,
        vo.visit_type_concept_id,
        vo.provider_id,
        vo.care_site_id,
        vo.visit_source_value,
        vo.visit_source_concept_id,
        vo.admitting_source_value,
        vo.admitting_source_concept_id,
        vo.discharge_to_source_value,
        vo.discharge_to_concept_id,
        vo.preceding_visit_occurrence_id,
        map.data_partner_id
--        vcn.concept_name as visit_concept_name,
--        vtcn.concept_name as visit_type_concept_name,
--        vscn.concept_name as visit_source_concept_name,
--        ascn.concept_name as admitting_source_concept_name,
--        dcn.concept_name as discharge_to_concept_name
    FROM `ri.foundry.main.dataset.f3d88333-9315-4a11-963a-6703a72cfd8a` vo
    JOIN   `/All of Us-cdb223/HIN - HIE/sharedResources/FullyIdentiifed/ccda/ccda_response_metadata` rm
      ON vo.filename = rm.response_file_path
    JOIN  `/All of Us-cdb223/HIN - HIE/sharedResources/health_care_site_to_data_partner_id` map
      ON rm.healthcare_site = map.healthcare_site
--    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` vcn
--      ON   vcn.concept_id = vo.visit_concept_id
--    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` vtcn
--      ON   vtcn.concept_id = vo.visit_type_concept_id
--    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` vscn
--      ON   vscn.concept_id = vo.visit_source_concept_id
--    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` ascn
--      ON   ascn.concept_id = vo.admitting_source_concept_id
--    JOIN `ri.foundry.main.dataset.831ad30e-a134-41ac-8f68-def86cc8b05c` dcn
--      ON   dcn.concept_id = vo.discharge_to_concept_i